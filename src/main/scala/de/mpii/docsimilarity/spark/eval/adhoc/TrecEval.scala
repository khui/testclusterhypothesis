package de.mpii.docsimilarity.spark.eval.adhoc

import java.io.{InputStreamReader, BufferedReader}

import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import de.mpii.evaltool._
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by khui on 03/01/16.
  */

object TrecEval{

  private var qreldir: String = ""
  private var baselinerankdir: String = "'"
  private var metric = "ndcg"
  private var expname = "expname0"
  private var subexpname = "nosubexpname"
  private var outputdir: String = ""
  private var topk: Int = 30
  private var parNumber: Int = 32
  private var cwyear: String = "cw4y"

  def parse(args: List[String]): Unit = args match {
    case ("-i") :: dir :: tail =>
      qreldir = dir
      parse(tail)
    case ("-b") :: dir :: tail =>
      baselinerankdir = dir
      parse(tail)
    case ("-m") :: mname :: tail =>
      metric = mname
      parse(tail)
    case ("-o") :: file :: tail =>
      outputdir = file
      parse(tail)
    case ("-c") :: coreNum :: tail =>
      parNumber = coreNum.toInt
      parse(tail)
    case ("-k") :: k :: tail =>
      topk = k.toInt
      parse(tail)
    case ("-y") :: year :: tail =>
      cwyear = year
      parse(tail)
    case ("-e") :: name :: tail =>
      expname = name
      parse(tail)
    case ("-s") :: name :: tail =>
      subexpname = name
      parse(tail)
    case (Nil) =>
  }


  def main(args: Array[String]) {
    parse(args.toList)
    val conf: SparkConf = new SparkConf()
      .setAppName("Eval runs with qrel with " + metric + "@" + topk + " for " + expname + "-" + subexpname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.max.mb", "1024")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)],
          classOf[Map[String, (Map[String, (Int, Int, Int)], Int, String)]],
          classOf[(Int, Int, Int)],
          classOf[Map[String, (Int, Int, Int)]],
          classOf[Seq[(Int, String, String)]],
          classOf[VectorizeDoc],
          classOf[(Int, String, String)]
        ))
    val sc: SparkContext = new SparkContext(conf)

    // check existence of output directory
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)


    val trecrundir = "/user/khui/data/trecruns/adhoc/normrun30qrel"

    val baselineRunidmap = baselinerankdir

    val eval = new TrecEval

    // read in trec runs to eval
    val yearRuns: Array[(String, Array[String])] = eval.readTrecruns(fs, trecrundir, topk)

    val convert2Javamap: (Array[(String, Double)] =>java.util.Map[java.lang.String, java.lang.Double])
    = (scalaArr : Array[(String, Double)]) => {
      mapAsJavaMap(scalaArr.map(p => p._1.toString->new java.lang.Double(p._2)).toMap)
    }

    // read in baseline run
    val yearRunidmap: Map[String, Array[(String, Double)]] = eval.readBaselineRunrank(sc, baselineRunidmap)

    // initialise the kendall's tau ap
    val yearKentauap: Map[String, KendalltauAP] = yearRunidmap.map(v => v._1 ->  new KendalltauAP(convert2Javamap(v._2)))

    val qrelstatus = fs.listFiles(new Path(qreldir), true)

    val qreldirid = new Path(qreldir).getName

    val outdir = outputdir + "/" + metric + "_" + expname + "_" + subexpname + "_" + qreldirid

    val qrelfiles = new ArrayBuffer[LocatedFileStatus]

    while(qrelstatus.hasNext){
      qrelfiles.append(qrelstatus.next())
    }

    println("qrelfiles " + qrelfiles.length)


    val qrels2test: Seq[(String, (EvalMetrics, String))] =
    qrelfiles
      .toSeq
      .par
      .filter(lfs => lfs.isFile)
      .map(lfs => lfs.getPath)
      .filter(p => !p.getName.startsWith("_"))
      .map(p =>  (p.getParent.getParent.getName + "_" + p.getParent.getName, p))
      .map{case(id, p) => (id, eval.qrel(fs, p, metric, id))}
      .seq

    println("qrels2test " + qrels2test.length)

    val qrelseval = qrels2test.map{case(id, p) => (id, p._1)}

    val idStat: Map[String, String] = qrels2test.map{case(id, p) => (id, p._2.substring(2))}.toMap

    val ktcorr = new KendallsCorrelation()

    val stator : StatGenerator = new StatGenerator

    val importantStatName = Array("expyear","expname","sr", "ktau","f1", "rmse","f1t", "ur",
      "simithresd","coverage","utility","contextweight","windowlen","vectortype","includequeryterm",
      "p", "pt", "r", "rt", "sn", "relr", "tn", "rtn")

    val outputlines: Array[(Any, Int)] =
    sc
      .parallelize(qrelseval, parNumber)
      .map{case(qrelid, qreleval) =>
        qrelid ->
       yearRuns
          .filter(v => v._1 == qrelid.split("_").last)
          .map(v =>  qreleval.getAvgScore(v._2, false))
          .map(kv => (kv.getKey, kv.getValue.toDouble))
          .toMap
      }
      .collect()
      .map{
        case(qrelid, runmaps) =>
          val year = qrelid.split("_").last
          val baselineRunidmap: Array[(String, Double)] = yearRunidmap(year)
          val bmaptmap =
          baselineRunidmap
            .filter{case(rid, map) => runmaps.contains(rid)}
            .map{case(rid, map) => (map, runmaps(rid))}
          val bmaps: Array[Double] = bmaptmap.map(v => v._1)
          val tmaps: Array[Double] = bmaptmap.map(v => v._2)

          val korrvalule = ktcorr.correlation(bmaps, tmaps)
          val korrap = yearKentauap(year).correlation(convert2Javamap(runmaps.toArray))
          val rmse = eval.computeRMSE(bmaps, tmaps)
        //  println(qrelid+"\n" + Array(year,qrelid, korrvalule, korrap, rmse).mkString("\t") +"\n"+ bmaptmap.map(t => t._1 + "\t" + t._2).mkString("\n"))
          (year, qrelid, korrvalule, korrap, rmse)
      }
      // year runid corr stat
      .map { case (year, expid, ktau, korrap, rmse) =>
      if (subexpname.equals("nosubexpname")) {
        val expparams = expid.split("_")
        if (expparams.length == 7) {
          stator.sevencols(expid, idStat, ktau, rmse)
        } else if (expparams.length == 8) {
          stator.eightcols(expid, idStat, ktau, rmse)
        } else if (expparams.length == 10) {
          stator.tencols(expid, idStat, ktau, rmse)
        } else if (expparams.length == 4) {
          stator.fourcols(expid, idStat, ktau, rmse)
        } else if (expparams.length == 6) {
          stator.sixcols(expid, idStat, ktau, rmse)
        }
      } else if (subexpname.equals("sumsimi")){
        stator.gradeinfersumsimi(expid, subexpname, idStat, korrap, ktau)
      } else if (subexpname.equals("normSimiTfidf")){
        stator.normSimiTfidf(expid, subexpname, idStat, korrap, ktau)
      }else if (subexpname.equals("binary")){
        stator.binary(expid, subexpname, idStat, korrap, ktau)
      } else {
        stator.default(expid, subexpname, idStat, korrap, ktau)
      }
    }
    .+:(stator.default())
    .zipWithIndex

    if (fs.exists(new Path(outdir))) {
      println(outdir + " is being removed")
      fs.delete(new Path(outdir), true)
    }


    sc
      .parallelize(outputlines, 1)
      .sortBy(v => v._2)
      .map(v => v._1)
      .saveAsTextFile(outdir)
  }

}


class StatGenerator extends java.io.Serializable{

  var lowcostContextStatName = Array("expyear","expname","sr", "ktauap","ktau","f1", "rmse","f1t", "ur",
    "simithresd","coverage","utility","contextweight","windowlen","vectortype","includequeryterm",
    "p", "pt", "r", "rt", "sn", "relr", "tn", "rtn")


  def default(expid:String=null, subexpname:String= null, idStat: Map[String, String]= null, kentauap:Double= -1, kentau:Double= -1): String ={

    val  statnames = ArrayBuffer(
      "expyear","subexpname","ktauap","ktau")
    if (expid != null) {
      val expparams = expid.split("_")
      val expyear = expparams.last
      val predstat = idStat.get(expid).get
      val stats: Map[String, String] =
        predstat.split(" ")
          .map(kv => kv.split(":"))
          .filter(kv => kv.length == 2)
          .map(kv => kv.head -> kv.last)
          .+:("ktau" -> "%.4f".format(kentau))
          .+:("ktauap" -> "%.4f".format(kentauap))
          .+:("expyear" -> expyear)
          .toMap

      stats
        .keySet
        .toArray
        .filter(n => !statnames.contains(n))
        .sorted
        .foreach(n => statnames.append(n))

      lowcostContextStatName = statnames.toArray

      lowcostContextStatName
        .map(n => stats.getOrElse(n, "-"))
        .mkString(",")
    } else {
      lowcostContextStatName.mkString(",")
    }
  }

  def binary(expid:String=null, subexpname:String= null, idStat: Map[String, String]= null, kentauap:Double= -1, kentau:Double= -1): String ={
    val lowcostContextStatName = Array(
      "expyear","subexpname","ktauap","ktau",
      "f11", "f12", "f13", "f14", "n1", "n2", "n3", "n4")
    if (expid != null) {
      val stats = scala.collection.mutable.Map[String, String]()
      stats += "expyear" -> expid
      stats += "subexpname" -> subexpname
      stats += "ktau" -> "%.4f".format(kentau)
      stats += "ktauap" -> "%.4f".format(kentauap)

      lowcostContextStatName
        .map(n => stats.getOrElse(n, "-"))
        .mkString(",")
    } else {
      lowcostContextStatName.mkString(",")
    }
  }

  def normSimiTfidf(expid:String=null, subexpname:String= null, idStat: Map[String, String]= null, kentauap:Double= -1, kentau:Double= -1): String ={
    val lowcostContextStatName = Array(
      "expyear","subexpname","ktauap","ktau",
      "corr", "ncorr",
      "avg", "navg",
      "p21", "np21",
      "p31", "np31",
      "p32", "np32",
      "p43", "np43",
      "p42", "np42",
      "p41", "np41",
      "simithresd","alpha","maxgrade", "windowlen","vectortype")
    if (expid != null) {
      val expparams = expid.split("_")
      val simithresd = expparams(1).toDouble
      val maxgrade = expparams(2).toDouble
      val alpha = expparams(3).toDouble
      val windowlen = expparams(4).toInt
      val vectortype = expparams(5)
      val expyear = expparams(6)
      val predstat = idStat.get(expid).get
      val stats: Map[String, String] =
        predstat.split("[^:] ")
          .map(kv => kv.split(": "))
          .filter(kv => kv.length == 2)
          .map(kv => kv.head -> "%.3f".format(kv.last.toDouble))
          .+:("ktau" -> "%.4f".format(kentau))
          .+:("ktauap" -> "%.4f".format(kentauap))
          .+:("alpha" ->  "%.4f".format(alpha))
          .+:("maxgrade" ->  "%.1f".format(maxgrade))
          .+:("subexpname" -> subexpname)
          .+:("vectortype" -> vectortype)
          .+:("simithresd" -> "%.2f".format(simithresd))
          .+:("windowlen" -> "%d".format(windowlen))
          .+:("vectortype" -> vectortype)
          .+:("expyear" -> expyear)
          .toMap
      lowcostContextStatName
        .map(n => stats.getOrElse(n, "-"))
        .mkString(",")
    } else {
      lowcostContextStatName.mkString(",")
    }
  }

  def gradeinfersumsimi(expid:String=null, subexpname:String= null, idStat: Map[String, String]= null, kentauap:Double= -1, kentau:Double= -1): String ={
    val lowcostContextStatName = Array("expyear","expname","ktauap","ktau", "corr",
      "f11", "f12", "f13", "f14", "n1", "n2", "n3", "n4",
      "simithresd","utility","windowlen","vectortype")
    if (expid != null) {
      val expparams = expid.split("_")
      val utility = expparams(0)
      val simithresd = expparams(1).toDouble
      val maxgrade = expparams(2).toDouble
      val windowlen = expparams(3).toInt
      val vectortype = expparams(4)
      val expyear = expparams(5)

      val predstat = idStat.get(expid).get
      val stats: Map[String, String] =
        predstat.split(" ")
          .map(kv => kv.split(":"))
          .map(kv => kv.head -> "%.3f".format(kv.last.toDouble))
          .+:("ktau" -> "%.4f".format(kentau))
          .+:("ktauap" -> "%.4f".format(kentauap))
          .+:("utility" -> utility)
          .+:("expname" -> subexpname)
          .+:("simithresd" -> "%.2f".format(simithresd))
          .+:("windowlen" -> "%d".format(windowlen))
          .+:("vectortype" -> "para2vec")
          .+:("expyear" -> expyear)
          .toMap
      lowcostContextStatName
        .map(n => stats.getOrElse(n, "-"))
        .mkString(",")
    } else {
      lowcostContextStatName.mkString(",")
    }
  }

  def sevencols(expid:String, idStat: Map[String, String], correlation:Double, rmse:Double): String ={
    val expparams = expid.split("_")
    val utility = expparams(0)
    val simithresd = expparams(1).toDouble
    val coverage = expparams(2).toDouble
    val contextweight = expparams(3).toDouble
    val windowlen = expparams(4).toInt
    val includequeryterm = expparams(5).toBoolean
    val expyear = expparams(6)
    val predstat = idStat.get(expid).get
    val stats: Map[String, String] =
      predstat.split(" ")
        .map(kv => kv.split(":"))
        .map(kv => kv.head -> "%.5f".format(kv.last.toDouble))
        .+:("ktau" -> "%.4f".format(correlation))
        .+:("rmse" -> "%.5f".format(rmse))
        .+:("utility" -> utility)
        .+:("expname" -> "tauf1test")
        .+:("simithresd" -> "%.2f".format(simithresd))
        .+:("coverage" -> "%.2f".format(coverage))
        .+:("contextweight" -> "%.2f".format(contextweight))
        .+:("windowlen" -> "%d".format(windowlen))
        .+:("includequeryterm" -> includequeryterm.toString)
        .+:("vectortype" -> "para2vec")
        .+:("expyear" -> expyear)
        .toMap
    lowcostContextStatName
      .map(n => stats.getOrElse(n, "-"))
      .mkString(",")
  }

  def eightcols(expid:String, idStat: Map[String, String], correlation:Double, rmse:Double): String ={
    val expparams = expid.split("_")
    val expname = expparams(0)
    val simithresd = expparams(1).toDouble
    val coverage = expparams(2).toDouble
    val contextweight = expparams(3).toDouble
    val windowlen = expparams(4).toInt
    val includequeryterm = expparams(5).toBoolean
    val vectortype = expparams(6)
    val expyear = expparams(7)
    val predstat = idStat.get(expid).get
    val stats: Map[String, String] =
      predstat.split(" ")
        .map(kv => kv.split(":"))
        .map(kv => kv.head -> "%.5f".format(kv.last.toDouble))
        .+:("ktau" -> "%.4f".format(correlation))
        .+:("rmse" -> "%.5f".format(rmse))
        .+:("expname" -> expname)
        .+:("simithresd" -> "%.2f".format(simithresd))
        .+:("coverage" -> "%.2f".format(coverage))
        .+:("contextweight" -> "%.2f".format(contextweight))
        .+:("windowlen" -> "%d".format(windowlen))
        .+:("includequeryterm" -> includequeryterm.toString)
        .+:("vectortype" -> vectortype)
        .+:("expyear" -> expyear)
        .toMap
    lowcostContextStatName
      .map(n => stats.getOrElse(n, "-"))
      .mkString(",")
  }

  def tencols(expid:String, idStat: Map[String, String], correlation:Double, rmse:Double): String ={
    val expparams = expid.split("_")
    val utility = expparams(0)
    val simithresd = expparams(1).toDouble
    val coverage = expparams(2).toDouble
    val contextweight = expparams(3).toDouble
    val windowlen = expparams(4).toInt
    val includequeryterm = expparams(5).toBoolean
    val vectortype = expparams(6)
    val expname = expparams(7) + "-" + expparams(8)
    val expyear = expparams(9)
    val predstat = idStat.get(expid).get
    val stats: Map[String, String] =
      predstat.split(" ")
        .map(kv => kv.split(":"))
        .map(kv => kv.head -> "%.5f".format(kv.last.toDouble))
        .+:("ktau" -> "%.4f".format(correlation))
        .+:("rmse" -> "%.5f".format(rmse))
        .+:("expname" -> expname)
        .+:("utility"-> utility)
        .+:("simithresd" -> "%.2f".format(simithresd))
        .+:("coverage" -> "%.2f".format(coverage))
        .+:("contextweight" -> "%.2f".format(contextweight))
        .+:("windowlen" -> "%d".format(windowlen))
        .+:("includequeryterm" -> includequeryterm.toString)
        .+:("vectortype" -> vectortype)
        .+:("expyear" -> expyear)
        .toMap
    lowcostContextStatName
      .map(n => stats.getOrElse(n, "-"))
      .mkString(",")
  }

  def fourcols(expid:String, idStat: Map[String, String], correlation:Double, rmse:Double): String ={
    val expparams = expid.split("_")
    val selectname: String = expparams(0)
    val vecname = expparams(1)
    val percent = expparams(2)
    val expyear = expparams(3)
    val predstat = idStat.get(expid).get
    val stats: Map[String, String] =
      predstat.split(" ")
        .map(kv => kv.split(":"))
        .map(kv => kv.head -> "%.5f".format(kv.last.toDouble))
        .+:("ktau" -> "%.4f".format(correlation))
        .+:("rmse" -> "%.4f".format(rmse))
        .+:("expname" -> (selectname + "," + vecname.toString + "," + percent.toString).toString)
        // .+:("vecname" -> vecname)
        //.+:("selectname" -> selectname)
        // .+:("selectgroup" -> percent)
        .+:("expyear" -> expyear)
        .toMap
    lowcostContextStatName
      .map(n => stats.getOrElse(n, "-"))
      .mkString(",")
  }

  def sixcols(expid:String, idStat: Map[String, String], correlation:Double, rmse:Double): String ={
    val expparams = expid.split("_")
    val selectname: String = expparams(1)
    val vecname = expparams(2)
    val expyear = expparams(5)
    val expname = expparams(3) + "-" + expparams(4)
    val predstat = idStat.get(expid).get
    val stats: Map[String, String] =
      predstat.split(" ")
        .map(kv => kv.split(":"))
        .map(kv => kv.head -> "%.5f".format(kv.last.toDouble))
        .+:("ktau" -> "%.4f".format(correlation))
        .+:("rmse" -> "%.4f".format(rmse))
        .+:("expname" -> Array(selectname,expname).mkString(","))
        .+:("vectortype" -> vecname)
        //.+:("selectname" -> selectname)
        .+:("expyear" -> expyear)
        .toMap
    lowcostContextStatName
      .map(n => stats.getOrElse(n, "-"))
      .mkString(",")
  }
}


class TrecEval extends java.io.Serializable{

  def trecruns(fs: FileSystem, trecrundir : String): Map[String, Array[Path]] ={
    val years: Array[String] = Array("wt11", "wt12", "wt13", "wt14")
    years
      .map(y => (y, trecrundir + "/" + y))
      .map{case(year, dir) =>  year -> fs.listStatus(new Path(dir)).map(fs => fs.getPath)}
      .toMap
  }

  def computeRMSE(pval: Array[Double], tval: Array[Double]): Double ={
    val indxs = pval.indices
    val msd = indxs.map(i => (pval(i) - tval(i))*(pval(i) - tval(i))).sum / indxs.length.toDouble
    math.sqrt(msd)
  }

  def qrel(fs: FileSystem, qrelpath : Path, metrics:String, qrelid:String="notgiven", topk:Int=30): (EvalMetrics, String) = {
    val qrelstream: FSDataInputStream = fs.open(qrelpath)
    val br : BufferedReader = new BufferedReader(new InputStreamReader(qrelstream))
    val lines = new ArrayBuffer[String]()
    while(br.ready()){
      lines.append(br.readLine())
    }
    br.close()
    val linesarr = lines.toArray[String]
    val statline = linesarr.filter(l => l.startsWith("#"))
    val statstr = statline.length match{
      case 0 => "#NOSTAT"
      case _ => statline.head
    }

    val evaluator = metrics match {
      case "map" => new EvalMap(lines.toArray[String].filter(l => !l.startsWith("#")), qrelid)
      case "erria" => new EvalErrIA(lines.toArray[String].filter(l => !l.startsWith("#")), topk, qrelid)
      case "ndcg" => new EvalNdcg(lines.toArray[String].filter(l => !l.startsWith("#")), topk, qrelid)
    }
    (evaluator, statstr)
  }


  def qrel(fs: FileSystem, qrelprefix : String, year : String): EvalMetrics = {
    val qrelstream: FSDataInputStream = fs.open(new Path(qrelprefix + "." + year))
    val br : BufferedReader = new BufferedReader(new InputStreamReader(qrelstream))
    val lines = new ArrayBuffer[String]()
    while(br.ready()){
      lines.append(br.readLine())
    }
    br.close()
    val evaluator = new EvalMap(lines.toArray[String])
    evaluator
  }

  def readBaselineRunrank(sc: SparkContext, runevaldir:String): Map[String, Array[(String, Double)]] ={
    Array("wt11", "wt12", "wt13", "wt14")
    .map{
        year =>
          val pathstr = runevaldir + "/" + year + "/part*"
          val runidMap =
          sc.textFile(pathstr)
            .map(line => line.split(" "))
            .filter(cols => cols.length == 2)
            .map{cols => cols(0) -> cols(1).toDouble}
            .collect()
        println("Read in " + runidMap.length + " for " + year)
        year -> runidMap
    }
    .toMap
  }

  def readTrecruns(fs: FileSystem, trecrundir : String, topk : Int=30): Array[(String, Array[String])] ={
    val yearTrecruns: Map[String, Array[Path]] = trecruns(fs, trecrundir)
    val yearRuns =
      Array("wt11", "wt12", "wt13", "wt14")
        .par
        .flatMap{
          year =>
            yearTrecruns(year)
              .map(p => p.getName -> fs.open(p))
              .map{case(fn, fsd) =>
                val br : BufferedReader = new BufferedReader(new InputStreamReader(fsd))
                val lines = new ArrayBuffer[String]()
                while(br.ready()){
                  val line = br.readLine()
                  val cols = line.split(" ")
                  if (cols(3).toInt <= topk){
                    lines.append(line)
                  }
                }
                br.close()
                year -> lines.toArray[String]
              }

        }
        .toArray
    println("yearRuns " + yearRuns.length)
    yearRuns
  }
}
