package de.mpii.docsimilarity.spark.doc2vec.filteredngram

import java.io.{ByteArrayInputStream, DataInputStream}

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.utils.CleanContentTxt
import de.mpii.docsimilarity.spark.doc2vec.{DocVec, VectorizeDoc}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

/**
  * Created by khui on 10/26/15.
  */
object DocTermid extends DocVec {
  private var inputdir: String = ""
  private var outputdir: String = ""
  private var parNumber: Int = 1024
  private var cwyear: String = "cw4y"


  def parse(args: List[String]): Unit = args match {
    case ("-i") :: dir :: tail =>
      inputdir = dir
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
      .setAppName("Output cwid-termids " + cwyear)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.max.mb","1024")
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
    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }


    val pathList: List[String] = fs
      .listStatus(new Path(inputdir))
      .filter(f => !f.getPath.getName.startsWith("_"))
      .map(f => f.getPath.toString)
      .toList

    val qids = cwyear match {
      case "cw09" => 101 to 200
      case "cw12" => 201 to 300
      case "cw4y" => 101 to 300
    }

    val years = cwyear match {
      case "cw09" => Array("cw09")
      case "cw12" => Array("cw12")
      case "cw4y" => Array("cw09", "cw12")
    }

    val psf: VectorizeDoc = new VectorizeDoc()
    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)
    val yearTermidxdfcf: Map[String, Map[String, (Int, Int, Int)]] =
      years
        .par
        .map(year => year -> termIdxDfCf(sc, year, parNumber))
        .toMap
        .seq


    val pathSlices = pathList
      .par
      .map(p => (p, psf.path2qid(p)))
      .filter(t2 => qids.toArray.contains(t2._2))
      .foreach(
        pathQid => {
          val path = pathQid._1
          val qid = pathQid._2
          val (year, collen, cwrecord) = psf.qid2yearrecord(qid)
          val parNumber = this.parNumber
          val outputdir = this.outputdir
          val termTidDfCf: Map[String, (Int, Int, Int)] = yearTermidxdfcf(year)
          val cwidTerms =
            sc
              // read in for each query
              .sequenceFile(path, classOf[IntWritable], classOf[BytesWritable], parNumber)
              .map { kv =>
                val cwdoc: ClueWebWarcRecord = Class.forName(cwrecord).newInstance().asInstanceOf[ClueWebWarcRecord]
                cwdoc.readFields(new DataInputStream(new ByteArrayInputStream(kv._2.getBytes)))
                (kv._1.get(), cwdoc.getDocid, cwdoc.getContent)
              }
              .filter(v => v._3 != null)
              .collect()
              .map{case(q, cwid,doccontent) => (q, cwid, Jsoup.parse(doccontent))}
              .filter(v => v._3 != null)
              .map{case(q, cwid, htmlDoc) => (q, cwid, cleanpipeline.cleanTxtList(htmlDoc, cwid, true))}
              .filter(v => v._3 != null)
              .map { case (q, cwid, termList) =>
                val terms = termList.toArray(Array.empty[String])
                val line =
                terms
                  .indices
                  .map(p => p -> terms(p))
                  .map{case(p, t) => p -> (t, termTidDfCf.getOrElse(t, (-1,-1,-1))._1)}
                  .map{case(p, (term, tid)) => tid + ":" + term + ":" + p}
                  .mkString(" ")
                cwid -> line
              }
          sc
            .parallelize(cwidTerms, 1)
            .sortBy(v => v._1)
            .map(v => v._1 + " " + v._2)
            .saveAsTextFile(outputdir + "/" + qid)

          val avgLen = cwidTerms.map(v => v._2.length).sum / cwidTerms.length.toDouble
          println("Finished: total docs " + cwidTerms.length + " with avgLength " + avgLen +" for query " + qid)
        }
      )
  }

}
