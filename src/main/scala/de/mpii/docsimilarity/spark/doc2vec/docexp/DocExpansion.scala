package de.mpii.docsimilarity.spark.doc2vec.docexp

import java.io.{ByteArrayInputStream, DataInputStream}

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.input.writable.IntTuple2Writable
import de.mpii.docsimilarity.mr.utils.CleanContentTxt
import de.mpii.docsimilarity.spark.doc2vec.{DocVec, ParseCwdoc, VectorizeDoc}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, FloatWritable, IntWritable}
import org.apache.mahout.math.VectorWritable
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable

/**
  * Created by khui on 10/26/15.
  */
object DocExpansion extends DocVec {
  private var inputdir: String = ""
  private var outputdir: String = ""
  private var similaritydir: String = ""
  private var parNumber: Int = 1024
  private var cwyear: String = "cw09"

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
    case ("-s") :: dir :: tail =>
      similaritydir = dir
      parse(tail)
    case ("-y") :: year :: tail =>
      cwyear = year
      parse(tail)
    case (Nil) =>
  }

  def loadTermSimilarity(sc: SparkContext, similarityfile: String): RDD[(Int, Map[Int, Double])] = {
    val xtidytidsimilarity: RDD[((Int, Int), Double)] =
      sc.sequenceFile(similarityfile, classOf[IntWritable], classOf[VectorWritable], parNumber)
        .map { case (rtid, tidSimilarities) => (rtid.get(), tidSimilarities.get().nonZeroes().asScala) }
        .flatMap { case (xtid, ytidSimilarity) => for (e <- ytidSimilarity) yield (xtid, e.index()) -> e.get() }
    val termRow: RDD[(Int, Map[Int, Double])] = xtidytidsimilarity
      .flatMap { case ((xi, yi), similarity) => Array(xi ->(yi, similarity), yi ->(xi, similarity)) }
      .aggregateByKey[mutable.HashMap[Int, Double]](mutable.HashMap.empty[Int, Double], parNumber)((a, t2) => a.+=(t2), (a1, a2) => a1 ++= a2)
      .map { case (xtid, dict) => (xtid, dict.toMap) }
    termRow
  }

  def loadTermSimilarityPerQuery(sc: SparkContext, similarityfile: String): RDD[(Int, Map[Int, Double])] = {
    val xtidytidsimilarity: RDD[((Int, Int), Double)] =
      sc.sequenceFile(similarityfile, classOf[IntTuple2Writable], classOf[FloatWritable], parNumber)
        .map { case (termids, similarity) => (termids.getFirst, termids.getSecond) -> similarity.get().toDouble }
    val termRow: RDD[(Int, Map[Int, Double])] = xtidytidsimilarity
      .flatMap { case ((xi, yi), similarity) => Array(xi ->(yi, similarity), yi ->(xi, similarity)) }
      .aggregateByKey[mutable.HashMap[Int, Double]](mutable.HashMap.empty[Int, Double], parNumber)((a, t2) => a.+=(t2), (a1, a2) => a1 ++= a2)
      .map { case (xtid, dict) => (xtid, dict.toMap) }
    termRow
  }

  /**
    * dotproduct for document and unique term, in the same sparse term representation
    *
    * @param termid
    * @param termidySim
    * @param docs
    * @return
    */
  def dtermDotProduct(termid: Int, termidySim: Map[Int, Double], docs: Map[String, Map[Int, Double]]): Seq[(String, (Int, Double))] = {
    val colwiseDotproduct: Seq[(String, (Int, Double))] =
      docs
        .map { case (cwid, termtfidf) => cwid ->(termid, dotproduct(termid, termtfidf, termidySim)) }
        .filter(t3 => t3._2._2 > 0)
        .toSeq
    colwiseDotproduct
  }

  def dotproduct(rtermid: Int, docvector: Map[Int, Double], termvector: Map[Int, Double]): Double = {
      docvector
        .filter { case (tid, tfidf) => termvector.contains(tid) }
        .map { case (tid, tfidf) => tfidf * termvector(tid) }
        .sum
  }



  def main(args: Array[String]) {
    parse(args.toList)
    val conf: SparkConf = new SparkConf()
      .setAppName("Expand doc-term matrix on " + this.cwyear)
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

    val inputdir: String = this.inputdir
    val similaritydir: String = this.similaritydir
    val parNumber: Int = this.parNumber
    val cwyear: String = this.cwyear
    val outputdir: String = this.outputdir


    val sc: SparkContext = new SparkContext(conf)

    // check existence of output directory
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)
    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }

    val pathList: Array[String] = fs
      .listStatus(new Path(inputdir))
      .filter(f => !f.getPath.getName.startsWith("_"))
      .map(f => f.getPath.toString)



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

    val yearTermidxdfcf: Map[String, Map[String, (Int, Int, Int)]] =
      years
        .par
        .map(year => year -> termIdxDfCf(sc, year, parNumber))
        .toMap
        .seq


    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)

    val pathSlices = pathList
      .par
      .map(p => (p, psf.path2qid(p)))
      .filter(t2 => qids.toArray.contains(t2._2))
      .foreach(
        pathQid => {
          val path = pathQid._1
          val qid = pathQid._2
          val (year, collen, cwrecord) = psf.qid2yearrecord(qid)
          println("Start " + qid)


         val cwidTerms: Map[String, Map[Int, Double]] =
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
              .map{case(q, cwid, htmlDoc) => (q, cwid, Option(cleanpipeline.cleanTxtStr(htmlDoc, cwid, true)))}
              .filter(t3 => t3._3.nonEmpty)
              .map { case (qid: Int, cwid: String, content: Option[String]) =>
                val termTfidfSeq = psf.docstr2TfidfSeq(content.get, collen, yearTermidxdfcf(year))
                (qid, cwid, termTfidfSeq)
              }
              .map(t3 => t3._2 -> t3._3.toMap)
              .toMap
              .seq

          val bcdocs = sc.broadcast(cwidTerms)

          println("Finished broadcast cwidTerms " + cwidTerms.size + " for " + qid)

          def mergeExpNOriginVector(originTfidf: Map[Int, Double], expTfidf: Map[Int, Double]): Map[Int, Double] = {
            originTfidf ++ expTfidf.map { case (tid, tfidf) => (tid, tfidf + originTfidf.getOrElse(tid, 0d)) }
          }

          val termTermSimRow: RDD[(Int, Map[Int, Double])] = loadTermSimilarityPerQuery(sc, similaritydir + "/" + qid + "/part*").cache()

          println("Finished readin termtermSimilarity " + termTermSimRow.count() + " for " + qid)

          termTermSimRow
            //Seq[(String, (Int, Double))]: cwid-(tid, dotproduct)
            .flatMap { case (tidx, tidySim) => dtermDotProduct(tidx, tidySim, bcdocs.value) }
            .aggregateByKey[mutable.HashMap[Int, Double]](mutable.HashMap.empty[Int, Double])((m, t2) => m.+=(t2), (m1, m2) => m1.++=(m2))
            .map(t2 => (t2._1, t2._2.toMap))
            // add the origin term weighting back to the vector
            .map { case (cwid, termtfidf) => (cwid, mergeExpNOriginVector(bcdocs.value(cwid), termtfidf)) }
            .map(cwtv => mkstring(cwtv._1, cwtv._2.toMap))
            .coalesce(1)
            .saveAsTextFile(outputdir + "/" + qid)
          bcdocs.destroy()
          termTermSimRow.unpersist()
          println("Finished all " + qid)
        }
      )
  }

}
