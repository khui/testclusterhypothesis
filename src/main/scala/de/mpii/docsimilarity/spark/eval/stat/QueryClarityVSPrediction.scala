package de.mpii.docsimilarity.spark.eval.stat

import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object QueryClarityVSPrediction{

  private var rootdir: String = ""
  private var outputdir: String = ""
  private var parNumber: Int = 32
  private var cwyear: String = "cw4y"

  def parse(args: List[String]): Unit = args match {
    case ("-i") :: dir :: tail =>
      rootdir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outputdir = file
      parse(tail)
    case ("-c") :: coreNum :: tail =>
      parNumber = coreNum.toInt
      parse(tail)
    case ("-y") :: year :: tail =>
      cwyear = year
      parse(tail)
    case (Nil) =>
  }


  def main(args: Array[String]) {
    parse(args.toList)
    val conf: SparkConf = new SparkConf()
      .setAppName("Query clarity vs prediction performance ")
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

    val statdir = rootdir

    val statusFS = fs.listFiles(new Path(statdir), true)

    //val outdir = "/user/khui/qddoc2vec/maxrepcontextqrels/sigir16/evalresults/queryclarityVSprediction"

    val queryRelClarity = "/user/khui/data/queryclarity/rel-40/part*"

    val queryAllClarity = "/user/khui/data/queryclarity/all-40/part*"

    val qidRelClarity =
    sc.textFile(queryRelClarity, parNumber)
      .map(line => line.split(" ")).map(cols => cols(0).toInt -> cols(1).toDouble).collectAsMap().toMap

    val qidAllClarity =
      sc.textFile(queryAllClarity, parNumber)
        .map(line => line.split(" ")).map(cols => cols(0).toInt -> cols(1).toDouble).collectAsMap().toMap


    val statfiles = new ArrayBuffer[LocatedFileStatus]

    while(statusFS.hasNext){
      statfiles.append(statusFS.next())
    }

    println("statfiles " + statfiles.length)


    val stats2test =
      statfiles
        .toSeq
        .par
        .filter(lfs => lfs.isFile)
        .map(lfs => lfs.getPath)
        .filter(p => !p.getName.startsWith("_"))
        .filter(p => p.getParent.getName.equals("statistic"))
        .map(p =>  (p.getParent.getParent.getName, p.toString))
        //id, p

    println("stats2test " + stats2test.length)

    val ktcorr = new KendallsCorrelation()

    val importantStatName = Array("expyear","expname","sr", "ktau","f1", "f1t", "ur",
      "simithresd","coverage","contextweight","windowlen","includequeryterm",
      "p", "pt", "r", "rt", "sn", "relr", "tn", "rtn")


    val utilityQidFeatures: Array[(String, (Int, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double))] =
    stats2test
      .par
      .flatMap{case(id, path) =>
          val utility = id.split("_").head
          sc
            .textFile(path, parNumber)
            .filter(line => !line.startsWith("#"))
            .map(line => line.split(","))
            //qid,labelrate,f1,simithread,simimin,simimax,simiavg,simiq25,simiq50,simiq75,simiq90,prec,recall
            // ,f1t,prect,recallt,labelnum,uncoveredrate,relrate,totalnum,rtotalnum
            .map(cols => (cols(0).toInt, cols(1).toDouble, cols(2).toDouble, cols(3).toDouble,
            cols(4).toDouble,cols(5).toDouble,cols(6).toDouble,cols(7).toDouble,cols(8).toDouble,cols(9).toDouble,cols(10).toDouble))
            .map{case(qid, labelrate, f1, simithread, simimin,simimax,simiavg,simiq25,simiq50,simiq75,simiq90)
            => utility -> (qid, labelrate, f1, simithread, simimin,simimax,simiavg,simiq25,simiq50,simiq75,simiq90, qidRelClarity(qid), qidAllClarity(qid))}
            .collect
            .toSeq
      }
      .seq
      .toArray

    val outputlines =
    utilityQidFeatures
      // utility, qid, (qid, simithread, labelrate) -> (relclarity, allclarity, f1,simimin,simimax,simiavg,simiq25,simiq50,simiq75,simiq90)
    .map(v => (v._1,v._2._1) -> Array(v._2._1, v._2._4, v._2._2, qidRelClarity(v._2._1), qidAllClarity(v._2._1),v._2._3,v._2._5,v._2._6,v._2._7,v._2._8,v._2._9,v._2._10,v._2._11))
    .map{case(utilityyear, yxs) => utilityyear -> yxs.mkString("\t")}
    .toBuffer



    val utilities = outputlines.map(v => v._1._1).distinct

    utilities
      .foreach(
         u =>
           outputlines.append(((u,0), "#qid\tsimithread\tlabelrate\trelClarity\tallClarity\tf1\tsimimin\tsimimax\tsimiavg\tsimiq25\tsimiq50\tsimiq75\tsimiq90"))
      )

    utilities
    .foreach{
      utility =>
        sc
          .parallelize(outputlines, 1)
          .filter{case(utilityqid, line) => utilityqid._1.equals(utility)}
          .sortBy(v => v._1)
          .map{case(utilityqid, line) => line}
          .saveAsTextFile(outputdir + "/" + utility)
    }





   /* val outputlinesF11 =
      utilityQidFeatures
        .map(v=>v._1)
        .distinct
        .map{
        utility =>

          val qidFeatures =
          utilityQidFeatures
            .filter(v => v._1 == utility)
            .map(v => v._2)
            //F1 == 1
            .filter(v => v._3 ==1)

           val qids = qidFeatures.map(v => v._1).distinct


           qids
              .map(qid => qidFeatures.filter(v => v._1 == qid).sortBy(v => v._2).head)
            .sortBy(v => v._5)
            .map(v =>
              Array(utility, v._1,
                "%.3f".format(v._12), "%.6f".format(v._13),
                "%.3f".format(v._5),"%.3f".format(v._6),"%.3f".format(v._7),"%.3f".format(v._8),"%.3f".format(v._9),"%.3f".format(v._10),"%.3f".format(v._11),
                "%.3f".format(v._2), "%.3f".format(v._4),"%.3f".format(v._3)).mkString("\t"))
            .+:(Array("#","docutility","qid",
              "relclarity","allcalrity",
              "simimin","simimax","simiavg","simiq25","simiq50","simiq75","simiq90",
              "labelrate","simithread","f1").mkString("\t"))
            .mkString("\n")
      }

    val outputlinesF195: Array[String] =
      utilityQidFeatures
        .map(v=>v._1)
        .distinct
        .map{
          utility =>

            val qidFeatures =
              utilityQidFeatures
                .filter(v => v._1 == utility)
                .map(v => v._2)
                //F1 == 1
                .filter(v => v._3 >=0.9)

            val qids = qidFeatures.map(v => v._1).distinct


            qids
              .map(qid => qidFeatures.filter(v => v._1 == qid).sortBy(v => v._2).head)
              .sortBy(v => v._5)
              .map(v =>
                Array(utility, v._1,
                  "%.3f".format(v._12), "%.6f".format(v._13),
                  "%.3f".format(v._5),"%.3f".format(v._6),"%.3f".format(v._7),"%.3f".format(v._8),"%.3f".format(v._9),"%.3f".format(v._10),"%.3f".format(v._11),
                  "%.3f".format(v._2), "%.3f".format(v._4),"%.3f".format(v._3)).mkString("\t"))
              .+:(Array("#","docutility","qid",
                "relclarity","allcalrity",
                "simimin","simimax","simiavg","simiq25","simiq50","simiq75","simiq90",
                "labelrate","simithread","f1").mkString("\t"))
              .mkString("\n")
        }*/

    //val outputlines = outputlinesF11 ++ outputlinesF195


  }
}
