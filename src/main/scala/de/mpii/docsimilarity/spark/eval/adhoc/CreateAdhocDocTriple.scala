package de.mpii.docsimilarity.spark.eval.adhoc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by khui on 01/11/15.
 */
object CreateAdhocDocTriple {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("Generate doc triple for eval")
    val sc: SparkContext = new SparkContext(conf)
    val inputdir: String = "/user/khui/data/qrel"
    val outputdir: String = "/user/khui/data/TripleSimilarity/adhoc"
    FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(inputdir))
      .filter(f => f.getPath.getName.startsWith("qrels.adhoc"))
      .map(f => f.getPath)
      .foreach(
        p => {
          val year = p.getName.split("\\.")(2)
          val qidrange: Range =
            year match {
              case "wt09" => 1 until 51
              case "wt10" => 51 until 101
              case "wt11" => 101 until 151
              case "wt12" => 151 until 201
              case "wt13" => 201 until 251
              case "wt14" => 251 until 301
            }
          for (qid <- qidrange) {
            Qrel2RelrelIrrelTriple(sc, p.toString, outputdir, qid)
          }
          println(p + " finished.")
        }

      )

  }

  def Qrel2RelrelIrrelTriple(sc: SparkContext, qrelf: String, outdir: String, qid: Int) = {
    val qrellines: RDD[(String, Int)] = sc.textFile(qrelf)
      .map(line => line.split(" "))
      .filter(cols => cols(0).toInt == qid)
      .map(cols => (cols(2), cols(3).toInt))
      .cache()
    val relLines = qrellines
      .filter(cols => cols._2 > 0)
      .zipWithIndex()
      .cache()

    val irrelLines = qrellines
      .filter(cols => cols._2 <= 0)

    relLines
      .cartesian(relLines)
      // upper triangle of all rel-rel doc pairs
      .filter{case(ra, rb) => ra._2 < rb._2}
      .map { case (ra, rb) => (ra._1, rb._1) }
      .cartesian(irrelLines)
      .map(triple =>
        List(triple._1._1._1, triple._1._2._1, triple._2._1,
          triple._1._1._2, triple._1._2._2, triple._2._2).mkString(" "))
      .coalesce(1)
      .saveAsTextFile(outdir + "/" + qid)
  }


}
