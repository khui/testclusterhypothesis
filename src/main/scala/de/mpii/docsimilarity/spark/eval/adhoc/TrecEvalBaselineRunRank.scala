package de.mpii.docsimilarity.spark.eval.adhoc

import java.io.{BufferedReader, InputStreamReader}

import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import de.mpii.evaltool.{EvalNdcg, EvalErrIA, EvalMetrics, EvalMap}
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by khui on 03/01/16.
  */

object TrecEvalBaselineRunRank{

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Create ground truth run ranking for adhoc/diversity")
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

    val qrelfile = "/user/khui/data/qrel/qrels.adhoc"

    val trecrundir = "/user/khui/data/trecruns/adhoc/nonav"
      //"/user/khui/data/trecruns/adhoc/normrun30qrel"

    val topk = 30

    val outdir = "/user/khui/data/trecruns/adhoc/nonav/runeval/ndcg" + topk
      //"/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/ndcg" + topk
      //"/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/erria" + topk
      //"/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/mapdiversityexpand" + topk

    val eval = new TrecEvalBaselineRunRank


    val yearTrecruns: Map[String, Array[Path]] = eval.trecruns(fs, trecrundir)

    val yearEvaluator = Array("wt11", "wt12", "wt13", "wt14").map(y => y -> eval.qrel(fs, qrelfile, "ndcg", y, topk)).toMap

    val yearRuns: mutable.Seq[(String, Array[String])] =
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
      .toSeq
      .seq

    println("Finished read in trec runs "  + yearRuns.size)

    try{
      val yearRunMaps =
      sc
        .parallelize(yearRuns)
        .map{case(year,lines) =>
          val kv= yearEvaluator(year).getAvgScore(lines, false)
          (year, kv.getKey, kv.getValue.toDouble)
        }
        .map{case(year, runid, map) => year -> (runid, map)}
        .aggregateByKey[Array[(String, Double)]](Array.empty[(String, Double)])((a, e)=>a.+:(e), (a1, a2) => a1.++(a2))
        .map{case(year, runidMaps) => year -> runidMaps.sortBy(v => -v._2).map(v => v._1 + " " + v._2).mkString("\n")}
        .cache()
      val years = yearRunMaps.keys.collect()

      years
        .foreach(
          year =>
            yearRunMaps.filter(v => v._1 == year).map(v => v._2).repartition(1).saveAsTextFile(outdir + "/" + year)
        )
    } catch {
      case ex : Exception =>
        println(ex.getLocalizedMessage)
        println(ex.getMessage)
        println(ex.toString)
        ex.printStackTrace()
    }
  }

}


class TrecEvalBaselineRunRank extends java.io.Serializable{

  def trecruns(fs: FileSystem, trecrundir : String): Map[String, Array[Path]] ={
    val years: Array[String] = Array("wt11", "wt12", "wt13", "wt14")
    years
      .map(y => (y, trecrundir + "/" + y))
      .map{case(year, dir) =>  year -> fs.listStatus(new Path(dir)).map(fs => fs.getPath)}
      .toMap
  }


  def qrel(fs: FileSystem, qrelprefix : String, metricname:String, year : String, topk:Int=30): EvalMetrics = {
    val qrelstream: FSDataInputStream = fs.open(new Path(qrelprefix + "." + year))
    val br : BufferedReader = new BufferedReader(new InputStreamReader(qrelstream))
    val lines = new ArrayBuffer[String]()
    while(br.ready()){
      lines.append(br.readLine())
    }
    br.close()
    val evaluator = metricname match {
      case "map" => new EvalMap(lines.toArray[String].filter(l => !l.startsWith("#")), year)
      case "erria" => new EvalErrIA(lines.toArray[String].filter(l => !l.startsWith("#")), year)
      case "ndcg" => new EvalNdcg(lines.toArray[String].filter(l => !l.startsWith("#")), topk, year)
    }
    evaluator
  }
}
