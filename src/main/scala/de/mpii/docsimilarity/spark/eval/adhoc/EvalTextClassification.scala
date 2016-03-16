package de.mpii.docsimilarity.spark.eval.adhoc

import java.util.concurrent.{Callable, ExecutorCompletionService}

import de.mpii.docsimilarity.mr.utils.GlobalConstants
import de.mpii.docsimilarity.spark.eval.EvalIOUtils
import de.mpii.docsimilarity.spark.eval.adhoc.SplitTrainTestData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by khui on 16/11/15.
  */
class EvalTextClassification(sc: SparkContext, featuref: String, qrel: Map[String, Int], qid:Int, dim: Int, parNumber: Int = 3) extends Callable[String]{

  override def call(): String ={
    val docs: RDD[(String, Vector)] = EvalIOUtils
      .readSparseCWDocs(sc, featuref, dim, parNumber)

    val bcQrel = sc.broadcast(qrel)
    val labeledDocs: RDD[(String, LabeledPoint)] = EvalIOUtils
      .createLabeledPoints(docs, bcQrel)

    val stt : SplitTrainTestData = new SplitTrainTestData()

    val splits: ((RDD[String], RDD[LabeledPoint]), (RDD[String], RDD[LabeledPoint]), Int, Int)
    //= stt.rndSplit(labeledDocs)
    = stt.maxRep(labeledDocs)

    val training: RDD[LabeledPoint] = (splits._1._2).cache()
    val test = splits._2._2

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    val predictionAndLabels: RDD[(Double, Double)] = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels)

    val f_neg: Double = metrics.fMeasure(0)
    val f_pos: Double = metrics.fMeasure(1)
    val f_macro = (f_neg + f_pos) / 2

    val result = List(qid, splits._3, splits._4, f_pos, f_neg, f_macro)

    System.out.println("Runtime results: " + result.mkString("\t"))
    bcQrel.destroy()

    result.mkString(" ")
  }

}

object EvalTextClassification {

  var docvecdir:String = ""
  var simidir:String = ""
  var qreldir:String = ""
  var outfile = ""
  var parNumber:Int = 100

  private def parse(args : List[String]) : Unit = args match {
    case ("-v") :: dir :: tail =>
      docvecdir = dir
      parse(tail)
    case ("-s") :: dir :: tail =>
      simidir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outfile = file
      parse(tail)
    case ("-q") :: file :: tail =>
      qreldir = file
      parse(tail)
    case ("-c") :: coreNum :: tail =>
      parNumber = coreNum.toInt
      parse(tail)
    case(Nil) =>
  }



  def main(args : Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("Eval on text classification")
      .set("spark.driver.maxResultSize", "3g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max.mb","1024")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, LabeledPoint)],
          classOf[(Int,Double)],
          classOf[(String, Vector)],
          classOf[LabeledPoint],
          classOf[LogisticRegressionModel],
          classOf[(Double, Double)],
          classOf[Vector]))
    val sc: SparkContext = new SparkContext(conf)

    parse(args.toList)

    val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
    val completionService = new ExecutorCompletionService[String](pool)

    def qid2year(qid : Int) : (String, String, Int) = {
      qid match {
        case x if 1 to 50 contains x => ("cw09", "wt09",GlobalConstants.CARDINALITY_CW09)
        case x if 51 to 100 contains x => ("cw09", "wt10",GlobalConstants.CARDINALITY_CW09)
        case x if 101 to 150 contains x => ("cw09", "wt11",GlobalConstants.CARDINALITY_CW09)
        case x if 151 to 200 contains x => ("cw09", "wt12",GlobalConstants.CARDINALITY_CW09)
        case x if 201 to 250 contains x => ("cw12", "wt13",GlobalConstants.CARDINALITY_CW12)
        case x if 251 to 300 contains x => ("cw12", "wt14",GlobalConstants.CARDINALITY_CW12)
      }
    }

    val qid2process = 101 to 106
    val qrelf = qreldir + "/qrels.adhoc.*"
    val qidQrel = EvalIOUtils
      .readAdhocQrel(sc, qrelf)

    for (qid <- qid2process) {
      val cwWtCollen = qid2year(qid)
      val featuref = docvecdir + "/" + qid + "*"
      //new EvalTextClassification(sc, featuref, qidQrel.get(qid).get, qid, cwWtCollen._3, 24).call()
      completionService.submit(new EvalTextClassification(sc, featuref, qidQrel.get(qid).get, qid, cwWtCollen._3, 24))
    }
    pool.shutdown()
    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    val out = fs.create(new Path(outfile))
    val err = fs.create(new Path(outfile + "_err"))
    for (qid <- qid2process) {
      try {
        val resultLine: String = completionService.take().get()
        if (resultLine != null) {
          out.writeChars(resultLine + "\n")
          out.flush()
        }
      } catch {
        case ex : Exception => System.out.println("Exception for query " + qid + "\n" + ex.toString)
      }
    }
    out.close()
    err.close()
  }

}
