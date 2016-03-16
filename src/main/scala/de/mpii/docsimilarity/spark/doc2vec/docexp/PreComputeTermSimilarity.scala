package de.mpii.docsimilarity.spark.doc2vec.docexp

import java.io.{File, IOException}

import de.mpii.docsimilarity.mr.input.writable.IntTuple2Writable
import de.mpii.docsimilarity.mr.utils.CleanContentTxt
import de.mpii.docsimilarity.mr.utils.io.WordVectorSerializer
import de.mpii.docsimilarity.spark.doc2vec.{DocVec, VectorizeDoc}
import org.apache.hadoop.fs._
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory


/**
  * Created by khui on 26/11/15.
  */
object PreComputeTermSimilarity{


  private var wordvectordir: String = ""
  private var outputdir: String = ""
  private var parNumber: Int = 1024
  private var cwyear: String = "cw12"
  private var simiThread: Double = 0.4


  def parse(args: List[String]): Unit = args match {
    case ("-v") :: dir :: tail =>
      wordvectordir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outputdir = file
      parse(tail)
    case ("-c") :: coreNum :: tail =>
      parNumber = coreNum.toInt
      parse(tail)
    case ("-t") :: threahold :: tail =>
      simiThread = threahold.toDouble
      parse(tail)
    case ("-y") :: year :: tail =>
      cwyear = year
      parse(tail)
    case (Nil) =>
  }

  @throws(classOf[IOException])
  private def loadW2v(sc: SparkContext, word2vecdir: String): Word2Vec = {
    val startTime: Long = System.currentTimeMillis
    val w2v: Word2Vec = WordVectorSerializer.loadGoogleModel(new File(word2vecdir), true)
    println("load the Google News word2vec successfully in " + (System.currentTimeMillis - startTime) / 1000.0 + " seconds")
    w2v
  }




  def main(args: Array[String]) {
    parse(args.toList)

    val wordvectordir = this.wordvectordir
    var outputdir: String = this.outputdir
    val parNumber: Int = this.parNumber
    val cwyear: String = this.cwyear
    val simiThread = this.simiThread

    val conf: SparkConf = new SparkConf()
      .setAppName("Precompute the term similarity for " + cwyear + " with threshold " + simiThread)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .set("spark.akka.frameSize", "512")
      //.set("spark.driver.maxResultSize", "10240")
      .set("spark.kryoserializer.buffer.max.mb", "1024")

      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)],
          classOf[(Int, Int, Int)],
          classOf[Map[String, (Int, Int, Int)]],
          classOf[IndexedRow],
          classOf[DocVec],
          classOf[VectorizeDoc],
          classOf[TokenizerFactory],
          classOf[Map[Int, Array[Float]]],
          classOf[Array[Float]]
        ))
    val sc: SparkContext = new SparkContext(conf)

    // check existence of output directory
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)


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


    outputdir = outputdir + "/" + simiThread + "/" + cwyear


    val cosineSimilarity: (Array[Float], Array[Float]) => Float = (a1: Array[Float], a2: Array[Float]) => {
      val mul3 =
        a1
          .indices
          .map(i => (a1(i) * a2(i), a1(i) * a1(i), a2(i) * a2(i)))
      val dotproduct: Float = mul3.map(v3 => v3._1).sum
      val a1norm: Float = Math.sqrt(mul3.map(v3 => v3._2).sum).toFloat
      val a2norm: Float = Math.sqrt(mul3.map(v3 => v3._3).sum).toFloat
      dotproduct / (a1norm * a2norm)
    }

    val l2normalize: (Array[Float]) => Array[Float] = (v: Array[Float]) => {
      val squarenorm =
        v
          .indices
          .map(i => v(i) * v(i))
          .sum
      val norm = Math.sqrt(squarenorm)
      val normedV =
        v
          .indices
          .map(i => (v(i) / norm).toFloat)
          .toArray
      normedV
    }

    val psf: VectorizeDoc = new VectorizeDoc()
    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)

    Array(233, 234)
    //qids
      .par
      .map(qid => (qid, wordvectordir + "/" + qid + "/part-00000"))
      .foreach(
        qidPath => {
          val qid = qidPath._1
          val termvectorfile = qidPath._2
          val cosineSimilarity: (Array[Float], Array[Float]) => Float = (norm1: Array[Float], norm2: Array[Float]) => {
              norm1
                .indices
                .map(i => norm1(i) * norm2(i))
                .sum
          }
          val normlizer: (Array[Float]) => Array[Float] = (v: Array[Float]) => {
            val norm =  Math.sqrt(v.map(e => e * e).sum)
            v.map(e => (e / norm).toFloat)
          }
          val termVectors =
            sc
              .textFile(termvectorfile, parNumber)
              .map(line => line.split(" "))
              .filter(cols => cols.length == 301)
              .map(cols => cols(0).toInt -> (1 until cols.length).map(idx => cols(idx).toFloat).toArray)
              .map(t2 => t2._1 -> normlizer(t2._2))
              .cache()

          println(qid + ": read in " + termVectors.count() + " rows from " + termvectorfile)

          val queryoutdir = outputdir + "/" + qid
          if (fs.exists(new Path(queryoutdir))) {
            println(queryoutdir + " is being removed")
            fs.delete(new Path(queryoutdir), true)
          }

          termVectors
            .cartesian(termVectors)
            .coalesce(parNumber * 16)
            .filter(t2 => t2._1._1 > t2._2._1)
            .map(t2 => (t2._1._1, t2._2._1) -> cosineSimilarity(t2._1._2, t2._2._2))
            .filter(pair => pair._2 >= simiThread)
            .map(t2 => (new IntTuple2Writable(t2._1._1, t2._1._2), new FloatWritable(t2._2.toFloat)))
            .repartition(1)
            .saveAsSequenceFile(queryoutdir, Some(classOf[GzipCodec]))



          termVectors.unpersist()
          println(qid + ": finished.")
        }
      )
  }
}
