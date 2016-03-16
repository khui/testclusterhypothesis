package de.mpii.docsimilarity.spark.doc2vec.docexp

import java.io.{ByteArrayInputStream, DataInputStream, File, IOException}

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.input.writable.IntTuple2Writable
import de.mpii.docsimilarity.mr.utils.CleanContentTxt
import de.mpii.docsimilarity.mr.utils.io.WordVectorSerializer
import de.mpii.docsimilarity.spark.doc2vec.baselines.TfidfOnehot._
import de.mpii.docsimilarity.spark.doc2vec.{DocVec, ParseCwdoc, VectorizeDoc}
import org.apache.hadoop.fs._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.{BytesWritable, FloatWritable, IntWritable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.models.word2vec.Word2Vec
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}
import org.jsoup.Jsoup


/**
  * Created by khui on 26/11/15.
  */
object SplitTermvectorPerquery {

  private var cwseqfiles: String = ""
  private var wordvectordir: String = ""
  private var outputdir: String = ""
  private var parNumber: Int = 1024
  private var cwyear: String = "cw12"


  def parse(args: List[String]): Unit = args match {
    case ("-i") :: dir :: tail =>
      cwseqfiles = dir
      parse(tail)
    case ("-v") :: dir :: tail =>
      wordvectordir = dir
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

  @throws(classOf[IOException])
  private def loadW2v(sc: SparkContext, word2vecdir: String): Word2Vec = {
    val startTime: Long = System.currentTimeMillis
    val w2v: Word2Vec = WordVectorSerializer.loadGoogleModel(new File(word2vecdir), true)
    println("load the Google News word2vec successfully in " + (System.currentTimeMillis - startTime) / 1000.0 + " seconds")
    w2v
  }


  def main(args: Array[String]) {
    parse(args.toList)

    val cwseqfiles: String = this.cwseqfiles
    val wordvectordir = this.wordvectordir
    var outputdir: String = this.outputdir
    val parNumber: Int = this.parNumber
    val cwyear: String = this.cwyear

    val conf: SparkConf = new SparkConf()
      .setAppName("Split term vector file for " + cwyear)
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

    val yearTermidxdfcf: Map[String, Map[String, (Int, Int, Int)]] =
      years
        .par
        .map(year => year -> termIdxDfCf(sc, year, parNumber, Some(new DefaultTokenizerFactory())))
        .toMap
        .seq


    outputdir = outputdir
    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }


    val psf: VectorizeDoc = new VectorizeDoc()
    val b2s = new ParseCwdoc


    val pathlist = (year: String) => {
      fs
        .listStatus(new Path(cwseqfiles + "/" + year))
        .filter(f => !f.getPath.getName.startsWith("_"))
        .map(f => f.getPath.toString)
    }

    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)


    years
      .map(year => pathlist(year))
      .par
      .foreach(
        paths =>
          paths
            .map(p => (p, psf.path2qid(p)))
            .filter(t2 => qids.toArray.contains(t2._2))
            .foreach(
              pathQid => {
                val path = pathQid._1
                val qid = pathQid._2
                val (year, collen, cwrecord) = psf.qid2yearrecord(qid)
                println("Start " + qid)

                val termids: Set[Int] =
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
                    .map{case(q, cwid, htmlDoc) => cleanpipeline.cleanTxtStr(htmlDoc, cwid, true)}
                    .flatMap (content =>
                      psf.docstr2Termids(content, collen, yearTermidxdfcf(year), Some(new DefaultTokenizerFactory()))
                    )
                    .toSet
                    .seq

                val bcTermids = sc.broadcast(termids)

                println(qid + ": read in " + termids.size  +" on " + cwyear)

                val termvectorfile = wordvectordir + "." + year
                sc
                  .textFile(termvectorfile, parNumber)
                  .map(line => (line.split(" ")(0).toInt, line))
                  .filter(cols => bcTermids.value.contains(cols._1))
                  .sortBy(t2 => t2._1)
                  .map(t2 => t2._2)
                  .repartition(1)
                  .saveAsTextFile(outputdir + "/" + qid)
                println(qid + ": finished.")
              }
            )
      )

  }
}
