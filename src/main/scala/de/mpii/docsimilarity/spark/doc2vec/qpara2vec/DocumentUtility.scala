package de.mpii.docsimilarity.spark.doc2vec.qpara2vec

import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by khui on 09/01/16.
  */

object DocumentUtility {

  private var outputdir: String = ""
  private var trecrundir: String = ""
  private var parNumber: Int = 32
  private var cwyear: String = "cw4y"

  def parse(args: List[String]): Unit = args match {
    case ("-o") :: file :: tail =>
      outputdir = file
      parse(tail)
    case ("-r") :: dir :: tail =>
      trecrundir = dir
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
      .setAppName("Compute document utility")
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



    val year= (qid : Int) =>  qid match{
      case q if 101 to 150 contains q => "wt11"
      case q if 151 to 200 contains q => "wt12"
      case q if 201 to 250 contains q => "wt13"
      case q if 251 to 300 contains q => "wt14"
    }

    val qids = 101 to 300

    val du : DocumentUtility = new DocumentUtility

    qids
    .par
    .foreach{
      qid =>
        val y = year(qid)
        val docidRunidRank: RDD[(String, (String, Int))] =
        sc
          .textFile(trecrundir + "/" + y + "/*")
          .map(line => line.split(" "))
          .filter(cols => cols.head.toInt == qid)
          .map(cols => cols(2) -> (cols.last, cols(3).toInt))
          .cache()

        val outdirqid = outputdir + "/" + qid

        if (fs.exists(new Path(outdirqid))){
          println(outdirqid + " is being removed")
          fs.delete(new Path(outdirqid), true)
        }

        val runids = docidRunidRank.map(v => v._2._1).collect().distinct

        val cwids = docidRunidRank.map(v => v._1).collect().distinct

        println("STAT " + qid + " runids " + runids.length + " cwids " + cwids.length)


         du.apPrior(docidRunidRank)
            .map(v => v._1 + " " + "%.6E".format(v._2))
            .repartition(1)
            .saveAsTextFile(outputdir + "/" + qid)
    }

  }
}



class DocumentUtility extends java.io.Serializable{

  // P(r) = 1/2n log(n/r)
  def apPrior(runs: RDD[(String, (String, Int))], topn:Int=30): RDD[(String, Double)] ={
    val cwidUtility: RDD[(String, Double)] =
    runs
      .map{case(cwid, (run, rank)) => cwid -> 1.0/(2*topn)* Math.log(topn/rank.toDouble)}
      .aggregateByKey[Double](0)((s1,s2) => s1+s2, (s1,s2) => s1+s2)
    cwidUtility
  }



}
