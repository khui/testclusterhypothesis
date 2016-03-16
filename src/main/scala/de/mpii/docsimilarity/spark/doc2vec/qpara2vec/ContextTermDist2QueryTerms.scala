package de.mpii.docsimilarity.spark.doc2vec.qpara2vec

import java.io.{ByteArrayInputStream, DataInputStream}

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.utils.{GlobalConstants, CleanContentTxt}
import de.mpii.docsimilarity.spark.doc2vec.{DocVec, VectorizeDoc}
import de.mpii.docsimilarity.spark.eval.EvalIOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

import scala.collection.immutable.IndexedSeq

/**
  * Created by khui on 10/26/15.
  */
object ContextTermDist2QueryTerms extends DocVec {
  private var inputdir: String = ""
  private var outputdir: String = ""
  private var queryfile: String = ""
  private var parNumber: Int = 1024
  private var cwyear: String = "cw4y"

  def parse(args: List[String]): Unit = args match {
    case ("-i") :: dir :: tail =>
      inputdir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outputdir = file
      parse(tail)
    case ("-q") :: file :: tail =>
      queryfile = file
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
      .setAppName("Compute termdist w.r.t. query terms")
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
    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }

    val pathList: List[String] = fs
      .listStatus(new Path(inputdir))
      .filter(f => !f.getPath.getName.startsWith("_"))
      .map(f => f.getPath.toString)
      .toList

    val qids =
      cwyear match {
        case "cw09" => 101 to 200
        case "cw12" => 201 to 300
        case "cw4y" => 101 to 300
      }

    val years =
      cwyear match {
        case "cw09" => Array("cw09")
        case "cw12" => Array("cw12")
        case "cw4y" => Array("cw09", "cw12")
      }

    val psf: VectorizeDoc = new VectorizeDoc()
    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)
    val yearTermidxdfcf =
      years
        .par
        .map(year => year -> termIdxDfCf(sc, year, parNumber))
        .toSeq
        .seq
        .toMap

    val termTermid: Map[String, Map[String, Int]] = yearTermidxdfcf
      .map { case (y, m) => y -> m.map(v => v._1 -> v._2._1) }

    val termidTerm: Map[String, Map[Int, String]] = yearTermidxdfcf
      .map{case(y, m) => y -> m.map(v => v._2._1 -> v._1)}

    val bctermidTerm = sc.broadcast(termidTerm)

    val queries =
      years
        .map(y => y -> EvalIOUtils.readQueries(queryfile, termTermid(y)))
        .toMap
    println("Read in queries: " + queries.size)

    pathList
      .par
      .map(p => (p, psf.path2qid(p)))
      .filter(t2 => qids.toSet.contains(t2._2))
      .foreach {
        pathQid => {
          val path = pathQid._1
          val qid = pathQid._2
          val (year, collen, cwrecord) = psf.qid2yearrecord(qid)
          val queryTerms: Array[Int] = queries(year)(qid)
          val parNumber = this.parNumber
          val outputdir = this.outputdir
          println("Start read doc for " + qid + " with length " + queryTerms.length)

          val cwidDocTerms: Array[(String, Array[Int])] =
            sc
              // read in for each query
              .sequenceFile(path, classOf[IntWritable], classOf[BytesWritable], parNumber)
              .map { kv =>
                val cwdoc: ClueWebWarcRecord = Class.forName(cwrecord).newInstance().asInstanceOf[ClueWebWarcRecord]
                cwdoc.readFields(new DataInputStream(new ByteArrayInputStream(kv._2.getBytes)))
                (kv._1.get(), cwdoc.getDocid, cwdoc.getContent)
              }
              //.filter(v => v._3 != null && cwids2consider.contains(v._2))
              .filter(v => v._3 != null && v._3.length < GlobalConstants.MAX_DOC_LENGTH)
              .collect()
              .map { case (q, cwid, doccontent) => (q, cwid, Jsoup.parse(doccontent)) }
              .filter(v => v._3 != null)
              .map { case (q, cwid, htmlDoc) => (q, cwid, cleanpipeline.cleanTxtList(htmlDoc, cwid, true)) }
              .map(t3 => t3._2 -> t3._3.toArray[String](Array.empty[String]))
              .map { case (cwid, terms) =>
                cwid ->
                  terms
                    .filter(term => term.length < GlobalConstants.MAX_TOKEN_LENGTH)
                    .filter(t => termTermid(year).contains(t))
                    .map(t => termTermid(year)(t))
              }
            sc
              .parallelize(cwidDocTerms, parNumber)
              .map { case (cwid, docterms) => cwidLine(cwid, docterms, queryTerms, bctermidTerm.value(year)) }
              .repartition(1)
              .saveAsTextFile(outputdir + "/" + qid)

            println("Finished for query " + qid)

          /**
            * For each document, for each term, generate term-[posdiff, posdiff...] w.r.t. all
            * query terms, retaining the smallest k distances
            *
            * @param cwid
            * @param docterms
            * @param queryTerms
            * @param termidTerm
            * @param closestK
            * @param maxdist
            * @return
            */
          def cwidLine(cwid: String, docterms: Array[Int], queryTerms:Array[Int], termidTerm :Map[Int, String], closestK: Int=5, maxdist :Int=20) : String = {

            val queryPos: Array[Int] =
              docterms
                .indices
                .map(pos => pos -> docterms(pos))
                .filter { case (pos, term) => queryTerms.contains(term) }
                .map{ case (pos, term) => pos}
                .toArray

            def term2QueryTermDistances(pos: Int): Array[Int] = {
              val dist2queryterm =
              queryPos
                .map(qpos => math.abs(qpos - pos))
                .filter(dist => dist <= maxdist)

              if(dist2queryterm.nonEmpty){
                dist2queryterm
                  .sorted
                  .slice(0, closestK)
              } else {
                Array.empty[Int]
              }
            }

            val tidDist2Queryterms: IndexedSeq[String] =
              docterms
                .indices
                .map(pos =>  pos -> docterms(pos))
                .map { case (pos, term) => (pos,term) -> term2QueryTermDistances(pos) }
                .filter(v => v._2.nonEmpty)
                .map{case ((pos, term), dists) => termidTerm(term) + ":" + dists.sorted.mkString(",")}

            tidDist2Queryterms.+:(cwid).mkString(" ")
          }

        }
      }
  }
}
