package de.mpii.docsimilarity.spark.doc2vec.baselines

import java.io.{ByteArrayInputStream, DataInputStream}

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.utils.{CleanContentTxt, GlobalConstants}
import de.mpii.docsimilarity.spark.doc2vec.docexp.DocExpansion._
import de.mpii.docsimilarity.spark.doc2vec.{DocVec, ParseCwdoc, VectorizeDoc}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

/**
  * Created by khui on 10/26/15.
  */

object DocMatrixSVD extends DocVec {
  private val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)


  private var inputdir: String = ""
  private var outputdir: String = ""
  private var parNumber: Int = 24
  private var cwyear: String = "cw09"
  private var dimk = 100

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
      .setAppName("Conduct SVD on doc-term matrix for " + cwyear + " to " + dimk + " dimension.")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)],
          classOf[Map[String, (Map[String, (Int, Int, Int)], Int, String)]],
          classOf[(Int, Int, Int)],
          classOf[Map[String, (Int, Int, Int)]],
          classOf[IndexedRow],
          classOf[DocVec],
          classOf[VectorizeDoc],
          classOf[BlockMatrix]
        ))
    val sc: SparkContext = new SparkContext(conf)

    // check existence of output directory
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)
    outputdir = outputdir + "/" + cwyear
    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }

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

    val yearTermidxdfcf: Map[String, Map[String, (Int, Int, Int)]] =
      years
        .par
        .map(year => year -> termIdxDfCf(sc, year, parNumber))
        .toMap
        .seq


    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)
    val pathList: List[String] = fs
      .listStatus(new Path(inputdir))
      .filter(f => !f.getPath.getName.startsWith("_"))
      .map(f => f.getPath.toString)
      .toList

    val psf: VectorizeDoc = new VectorizeDoc()


    println("pathList to process " + pathList.size + " qids to process " + qids.size)

    val pathSlices = pathList
      .par
      .map(p => (p, psf.path2qid(p)))
      .filter(t2 => qids.toArray.contains(t2._2))
      .map(
        pathQid => {
          val path = pathQid._1
          val qid = pathQid._2
          val (year, collen, cwrecord) = psf.qid2yearrecord(qid)
          println("Start " + qid + " on " + pathQid._1)

          val cwidTerms: Seq[(String, Map[Int, Double])] =
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
              .map { case (q, cwid, doccontent) => (q, cwid, Jsoup.parse(doccontent)) }
              .map { case (q, cwid, htmlDoc) => (q, cwid, cleanpipeline.cleanTxtStr(htmlDoc, cwid, true)) }
              .map { case (qid: Int, cwid: String, content: String) =>
                val termTfidfSeq = psf.docstr2TfidfSeq(content, collen, yearTermidxdfcf(year))
                (qid, cwid, termTfidfSeq)
              }
              .map(t3 => t3._2 -> t3._3.toMap)
              .toSeq

          println("Debug qidCwidTermVector " + qid)
        }
      )

        /*  val cwidIdxTermTfidf: RDD[(String, (Long, linalg.Vector))] =
            sc
              .parallelize(cwidTerms)
              .zipWithIndex()
              .map(t2 => t2._1._1 ->(t2._2, t2._1._2))
              .cache()

          println("Debug cwidIdxTermTfidf " + qid)

          val idxCwid: Map[Long, String] = cwidIdxTermTfidf
            .map(v => v._2._1 -> v._1)
            .collectAsMap()
            .toMap

          println("Constructing Matrix: for query " + qid + " for " + idxCwid.size + " docs")

          val idxRows: RDD[IndexedRow] = cwidIdxTermTfidf
            // RDD[IndexedRow]
            .map(v => new IndexedRow(v._2._1, v._2._2))
            .persist(StorageLevel.MEMORY_AND_DISK_SER)

         // println("Debug idxRows: " + idxRows.count() + " for query " + qid)

          val docTermMatrix =
            new IndexedRowMatrix(idxRows)

          println("Debug docTermMatrix: " + docTermMatrix.numRows() + " " + docTermMatrix.numCols() + " for query " + qid)

          val termDocMattrix: IndexedRowMatrix = docTermMatrix.toCoordinateMatrix().transpose.toIndexedRowMatrix()

          println("Debug: termDocMattrix " + termDocMattrix.numRows() + " " + termDocMattrix.numCols() + " for query " + qid)
          cwidIdxTermTfidf.unpersist()
         // idxRows.unpersist()

          (termDocMattrix,  idxCwid, qid)


        }
      )
      .seq
      .map {
        case (termDocMattrix, idxCwid, qid) =>
          val svdMatrix: SingularValueDecomposition[IndexedRowMatrix, Matrix] = termDocMattrix.computeSVD(dimk, computeU = true)

          println("Debug: svdMatrix " + svdMatrix.s.size + " for query " + qid)

          val V: Matrix = svdMatrix.V

          println("SVD is done: " + V.numRows + " " + V.numCols + " for query " + qid)
          (V, idxCwid, qid)

      }
      .par
      .foreach {
        case (v, idxCwid, qid) => {
          val outResult = v
            .toArray
            .grouped(dimk)
            .toArray
            .zipWithIndex

          println("Debug: outResult " + outResult.size)


          sc.parallelize(outResult)
            .map(aidx => Array(idxCwid.get(aidx._2).get, aidx._1.zipWithIndex.map(i => i._2 + ":" + i._1).mkString(" ")).mkString(" "))
            .coalesce(1)
            .saveAsTextFile(outputdir + "/" + qid)



          println("Debug: output finished")*/


          //println("Finished " + qid)
       // }

     // }
  }
}

