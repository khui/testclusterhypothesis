package de.mpii.docsimilarity.spark.eval.adhoc

import java.io.{BufferedReader, InputStreamReader}
import java.lang

import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by khui on 03/01/16.
  */

object TrecEvalBaselineRRankPerquery{

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Create ground truth run ranking for adhoc/diversity per query")
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

    val qrelfile = "/user/khui/data/qrel/qrels.diversity"

    val trecrundir = "/user/khui/data/trecruns/adhoc/normrun30qrel"

    val topk = 30

    val outdir = "/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/mapdiversityexpand" + topk + "/perquery"
      //"/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/mapdiversityexpand" + topk
      //"/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/erria" + topk
      //"/user/khui/data/trecruns/adhoc/normrun30qrel/runeval/mapdiversityexpand" + topk

    val eval = new TrecEvalBaselineRunRank


    val yearTrecruns: Map[String, Array[Path]] = eval.trecruns(fs, trecrundir)

    val yearEvaluator = Array("wt11", "wt12", "wt13", "wt14").map(y => y -> eval.qrel(fs, qrelfile, "map", y)).toMap

    val yearRuns =
    Array("wt11", "wt12", "wt13", "wt14")
      .par
      .flatMap{
      year =>
       yearTrecruns(year)
          .map(p => p.getName -> fs.open(p))
          .map{case(fn, fsd) =>
              val br : BufferedReader = new BufferedReader(new InputStreamReader(fsd))
              val lines = new ArrayBuffer[(Int, String)]()
              while(br.ready()){
                val line = br.readLine()
                val cols = line.split(" ")
                if (cols(3).toInt <= topk){
                  val qid = line.split(" ").head.toInt
                  lines.append((qid, line))
                }
              }
              br.close()
            year -> lines.toArray[(Int, String)]
            }

      }
      .toSeq
      .seq

/*
    sc
      .parallelize(yearRuns)
      .foreach{case(year,lines) =>
        println(yearEvaluator(year).getMAPPerQuery(lines, false))
      }
*/

    println("Finished read in trec runs "  + yearRuns.size)

    try{
      val qidRunMaps: RDD[(Int, String)] =
      sc
        .parallelize(yearRuns)
        .flatMap{case(year,qidlines) =>
          val qids = year match{
            case "wt09" => 1 to 50
            case "wt10" => 51 to 100
            case "wt11" => 101 to 150
            case "wt12" => 151 to 200
            case "wt13" => 201 to 250
            case "wt14" => 251 to 300
          }
          qids
          .map(qid => qid -> qidlines.filter(v => v._1 == qid).map(v => v._2))
          .map{case(qid,lines) => (qid, yearEvaluator(year).getScore(lines, qid, false))}
          .map{case(qid, kv) => (year, qid, kv.getKey, kv.getValue.toDouble)}
        }
        .map{case(year, qid, runid, ap) => qid -> (runid, ap)}
        .aggregateByKey[Array[(String, Double)]](Array.empty[(String, Double)])((a, e)=>a.+:(e), (a1, a2) => a1.++(a2))
        .map{case(qid, runidMaps) => qid -> runidMaps.sortBy(v => -v._2).map(v => v._1 + " " + v._2).mkString("\n")}
        .cache()
      val qids = qidRunMaps.keys.collect()

      qids
        .foreach(
          qid =>
            qidRunMaps.filter(v => v._1 == qid).map(v => v._2).repartition(1).saveAsTextFile(outdir + "/" + qid)
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



