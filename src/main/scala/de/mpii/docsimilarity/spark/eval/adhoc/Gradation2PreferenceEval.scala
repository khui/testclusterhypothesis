package de.mpii.docsimilarity.spark.eval.adhoc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection
import scala.collection.parallel.immutable.ParSeq

/**
  * Created by khui on 29/02/16.
  */
object Gradation2PreferenceEval {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Gradation Inference Eval: Preference Evaluation")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.max.mb", "1024")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)]
        ))
    val sc: SparkContext = new SparkContext(conf)

    val jobconf = new JobConf(sc.hadoopConfiguration)

    // check existence of output directory
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)

    val trecQrel = "/user/khui/data/qrel/qrels.adhoc.wt*"
    val outputdir = ""
    val dir2test = "/user/khui/qddoc2vec/filteredngram/gradedinfer/tfidf/unsupervised"


    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }

    val mapAddTuple: ((Map[Int, Array[String]], (Int, String)) => Map[Int, Array[String]]) = (m, t) => {
      m + (t._1 -> m.getOrElse(t._1, Array.empty[String]).+:(t._2))
    }

    val tasks = Array("adhoc", "diversity")
    val maxgrades = Array("0.0", "4.0")
    val selectnums = Array(20, 40, 60, 80, 100, 120, 140, 160, 180, 200,
                    220, 240, 260, 280, 300, 350, 400, 450, 500, 600,
      700, 800, 900, 1000, 2000, 3000, 5000, 8000)


    val params: Array[(String, String, Int)] = for (task <- tasks; maxgrade <- maxgrades; selectnum <- selectnums) yield {(task, maxgrade, selectnum)}


    val purityEval : (((Double, Double),(Double, Double))=>Int) = (dh,dl) =>{
      if(dh._2 > dl._2) 1 else 0
    }

    val compEval : (((Double, Double),(Double, Double))=>Int) = (dh,dl) =>{
      if(dh._1 > dl._1) 1 else 0
    }

    val hybridEval : (((Double, Double),(Double, Double))=>Int) = (dh,dl) =>{
      if (purityEval(dh, dl) + compEval(dh, dl) > 0) 1 else 0
    }

    val qids = 101 to 300

    val qid2year: (Int=>String) =(queryid) => {
      queryid match {
        case qid if (101 to 150).contains(qid) => "wt11"
        case qid if (151 to 200).contains(qid) => "wt12"
        case qid if (201 to 250).contains(qid) => "wt13"
        case qid if (251 to 300).contains(qid) => "wt14"
        case _ => "wtunknown"
      }
    }

    val evalresults: Seq[((String, String, Int), (String, Int, String, Double, Double, Double))] =
    qids
      .par
      .flatMap(
          qid => {
            val readQrels:((SparkContext, String, Int) => RDD[(String, Double)]) = (sc, qrelfile, qid) =>{
              sc
                .textFile(qrelfile)
                .map(line => line.split(" "))
                .filter(cols => cols.head.toInt == qid)
                .filter(cols => cols.last.toInt > 0)
                .map(cols => cols(2)-> cols.last.toFloat)
            }

            val qrels: RDD[(String, Double)] = readQrels(sc, trecQrel, qid)

            val pairkeyHLCwids: Array[(String, Array[(String, String)])] =
            qrels
              .cartesian(qrels)
              .filter{case((cwid1,l1), (cwid2, l2)) => l1 > l2}
              .map{case((cwid1,l1), (cwid2, l2)) => l1 + "_" + l2 -> (cwid1, cwid2)}
              .groupByKey()
              .map{case(pairkey, cwids) => pairkey -> cwids.toArray}
              .collect()
 //           val a: Array[((String, String, Int), (Int, String, Double, Double, Double))] =
            params
              .map{case(task, maxgrade, selectnum) =>  (task, maxgrade, selectnum) -> Array("tfidf","LmdRel2Nrel",task, "10000", "2", selectnum, maxgrade, "").mkString("_") }
              .flatMap{
                case((task, maxgrade, selectnum), filenameprefix) => {

                  val methodCwidVal: Array[RDD[(String, String, Double)]] =
                    Array("comp", "pure")
                      .map(m => readQrels(sc, dir2test + "/" + filenameprefix + m + "/wt*/part*", qid).map(t2 => (m, t2._1, t2._2)))
                  val cwidCompPure: Map[String, (Double, Double)] =
                  methodCwidVal
                      .head
                      .cartesian(methodCwidVal.last)
                      .filter { case (comp, pure) => comp._2 == pure._2 }
                      .map { case (comp, pure) => comp._2->( comp._3, pure._3) }
                      .collectAsMap()
                      .toMap
                  pairkeyHLCwids
                    .map{case(expid, cwidhlPairs) =>
                      val compPurityHybrid =
                      cwidhlPairs
                        .map{case(dh, dl) => (cwidCompPure.getOrElse(dh, (0.0, 0.0)),cwidCompPure.getOrElse(dl, (0.0, 0.0)))}
                        .map{case(dh, dl) => (compEval(dh, dl), purityEval(dh, dl), hybridEval(dh, dl))}
                      val compRatio = compPurityHybrid.map(t3 => t3._1).sum / cwidhlPairs.length.toDouble
                      val pureRatio = compPurityHybrid.map(t3 => t3._2).sum / cwidhlPairs.length.toDouble
                      val hybridRatio = compPurityHybrid.map(t3 => t3._3).sum / cwidhlPairs.length.toDouble
                      (task, maxgrade, selectnum) -> (qid2year(qid), qid, expid, compRatio, pureRatio, hybridRatio)
                    }
                }
              }
          }
    )
    .seq

    sc
      .parallelize(evalresults, 1)
      .groupByKey()
      .flatMap { case (expid, yearqidResults) =>
        yearqidResults
          .toArray
          .groupBy(t6 => t6._1)
          .map { case (year, res) =>
            val comp = res.map(t6 => t6._4).sum / res.length.toDouble
            val pure = res.map(t6 => t6._5).sum / res.length.toDouble
            val hybrid = res.map(t6 => t6._6).sum / res.length.toDouble
            Array(expid._1,expid._2, expid._3, year, comp, pure, hybrid).mkString(" ")
          }
      }
      .saveAsTextFile("")



  }
}
