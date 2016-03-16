package de.mpii.docsimilarity.spark.eval.adhoc

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by khui on 29/02/16.
  */
object CreateNoNAVRuns {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Create no-nav runs")
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

    val originalTrecRuns = "/user/khui/data/trecruns/adhoc/normrun30qrel"
    val qrelfilePrefix = "/user/khui/data/qrel/qrels.adhoc.wt*"
    val outputdir = "/user/khui/data/trecruns/adhoc/nonav"


    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }

    val mapAddTuple: ((Map[Int, Array[String]], (Int, String)) => Map[Int, Array[String]]) = (m, t) => {
      m + (t._1 -> m.getOrElse(t._1, Array.empty[String]).+:(t._2))
    }

    val queryidNavs: Map[Int, Array[String]] =
      sc
        .textFile(qrelfilePrefix)
        .map(line => line.split(" "))
        .filter(cols => (101 to 300).contains(cols.head.toInt))
        // in wt11 the nav is 3, meanwhile in wt12, 13, 14 the nav is 4
        .filter(cols => cols.last.toInt == 4 || ((101 to 150).contains(cols.head.toInt) && cols.last.toInt == 3))
        .map(cols => cols.head.toInt -> cols(2))
        .collect()
        .foldLeft[Map[Int, Array[String]]](Map.empty[Int, Array[String]])(mapAddTuple)
        .map { case (qid, cwids) => qid -> cwids.distinct }

    val qid2year: (Int=>String) =(queryid) => {
      queryid match {
        case qid if (101 to 150).contains(qid) => "wt11"
        case qid if (151 to 200).contains(qid) => "wt12"
        case qid if (201 to 250).contains(qid) => "wt13"
        case qid if (251 to 300).contains(qid) => "wt14"
        case _ => "wtunknown"
      }
    }

    sc
      .textFile(originalTrecRuns + "/wt*/input*", 24)
      .map(line => line.split(" "))
      .filter(cols => cols.length == 6)
      .filter(cols => (101 to 300).contains(cols.head.toInt))
      .map(cols => (cols.head.toInt, cols(2), cols(3).toInt, cols(4), cols.last))
      .filter { case (qid, cwid, rank, score, systemid) => !queryidNavs.getOrElse(qid, Array.empty).contains(cwid) }
      .groupBy(t5 => (t5._5, t5._1))
      .map { case ((sysid, qid), lines) =>
        qid2year(qid) + "/" + sysid ->
          (qid, lines.toArray.sortBy(t5 => t5._3).zipWithIndex.map { case (t5, idx) => Array(t5._1, "Q0", t5._2, idx + 1, t5._4, t5._5).mkString(" ") })
      }
      .groupBy(t2 => t2._1)
      .map { case (sysid, qidLines) => sysid -> qidLines.toArray.map(t2 => t2._2).sortBy { case (qid, lines) => qid }.flatMap { case (qid, lines) => lines } }
      .map{case(sysid, lines) => (NullWritable.get(), new Text(sysid + "/" + lines.mkString("\n").trim))}
      .saveAsHadoopFile(outputdir, classOf[NullWritable], classOf[Text], classOf[FileGroupingTextOutputFormat], jobconf)
  }

  class FileGroupingTextOutputFormat extends MultipleTextOutputFormat[NullWritable, Text] {

    override def generateActualValue(key: NullWritable, value: Text): Text = {
      new Text(value.toString.split("/").last)
    }

    override def generateFileNameForKeyValue(key: NullWritable, value: Text, name: String): String = {
      val year = value.toString.split("/").head
      val runid = value.toString.split("/")(1)
      year + "/input." + runid + ".nonav"
    }

  }


}
