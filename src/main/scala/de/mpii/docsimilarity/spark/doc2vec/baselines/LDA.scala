package de.mpii.docsimilarity.spark.doc2vec.baselines

import java.io.{ByteArrayInputStream, DataInputStream}

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.utils.{CleanContentTxt, GlobalConstants}
import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import de.mpii.docsimilarity.spark.doc2vec.baselines.DocMatrixSVD._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup


  object LDA {

    private var seqdocdir: String = ""
    private var outdir = ""
    private var qreldir: String = ""
    private var parNum = 1024
    private var numTopics = 20
    private var cwyear: String = "cw4y"


    private def parse(args: List[String]): Unit = args match {
      case ("-v") :: dir :: tail =>
        seqdocdir = dir
        parse(tail)
      case ("-o") :: file :: tail =>
        outdir = file
        parse(tail)
      case ("-c") :: coreNum :: tail =>
        parNum = coreNum.toInt
        parse(tail)
      case ("-t") :: topicnum :: tail =>
        numTopics = topicnum.toInt
        parse(tail)
      case ("-q") :: qrel :: tail =>
        qreldir = qrel
        parse(tail)
      case ("-y") :: year :: tail =>
        cwyear = year
        parse(tail)
      case (Nil) =>
    }

    val years = cwyear match {
      case "cw09" => Array("cw09")
      case "cw12" => Array("cw12")
      case "cw4y" => Array("cw09", "cw12")
    }


    def main(args: Array[String]): Unit = {
      parse(args.toList)
      val conf: SparkConf = new SparkConf()
        //.setAppName("LDA on cw4y (diversity) " + numTopics)
        .setAppName("LDA on cw4y " + numTopics)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
        .registerKryoClasses(
          Array(classOf[String],
            classOf[(String, String)],
            classOf[(Int, String)],
            classOf[(String, Double)]))
      val sc: SparkContext = new SparkContext(conf)

      val yearTermidxdfcf: Map[String, Map[String, (Int, Int, Int)]] =
        years
          .par
          .map(year => year -> termIdxDfCf(sc, year, parNum))
          .toMap
          .seq

    //val qidRelCwids: Map[Int, Set[String]] = readDiversityQrel(sc,qreldir + "/qrels.diversity.wt*", parNum)

      // check existence of output directory
      val fs: FileSystem = FileSystem
        .get(sc.hadoopConfiguration)
      if (fs.exists(new Path(outdir))) {
        println(outdir + " is being removed")
        fs.delete(new Path(outdir), true)
      }

      val pathList: List[String] = fs
        .listStatus(new Path(seqdocdir))
        .filter(f => !f.getPath.getName.startsWith("_"))
        .map(f => f.getPath.toString)
        .toList

      val psf: VectorizeDoc = new VectorizeDoc()
      val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)
      val qids = (101 to 300).toSet

      val pathSlices = pathList
        .par
        .map(p => (p, psf.path2qid(p)))
        .filter(t2 => qids.contains(t2._2))
        .foreach(
          pathQid => {

            val path = pathQid._1
            val qid = pathQid._2
            val (year, collen, cwrecord) = psf.qid2yearrecord(qid)

            println("Start " + qid)

            val qidCwidContent: Array[(Int, String, String)] =
              sc
                // read in for each query
                .sequenceFile(path, classOf[IntWritable], classOf[BytesWritable], parNum)
                .map { kv =>
                  val cwdoc: ClueWebWarcRecord = Class.forName(cwrecord).newInstance().asInstanceOf[ClueWebWarcRecord]
                  cwdoc.readFields(new DataInputStream(new ByteArrayInputStream(kv._2.getBytes)))
                  (kv._1.get(), cwdoc.getDocid, cwdoc.getContent)
                }
                .filter(v => v._3 != null && v._3.length < GlobalConstants.MAX_DOC_LENGTH)
                .collect()
                .map { case (q, cwid, doccontent) => (q, cwid, Jsoup.parse(doccontent)) }
                .map { case (q, cwid, htmlDoc) => (q, cwid, cleanpipeline.cleanTxtStr(htmlDoc, cwid, true)) }
             // only for diversity
         // .filter(v => qidRelCwids(qid).contains(v._2))



            val idxCwidContent: Array[(Long, (String, String))] =
              qidCwidContent
                .zipWithIndex
                .map(_.swap)
                .map(v => v._1.toLong ->(v._2._2, v._2._3))

            val idxCwid: Map[Long, String] =
              idxCwidContent
                .map(v => v._1 -> v._2._1)
                .toMap

            val idxContent =
              idxCwidContent
                .map(v => v._1 -> v._2._2)
                .map(v => v._1 -> psf.docstr2TfidfVector(v._2, collen, yearTermidxdfcf(year)))

            val idxDoces: RDD[(Long, Vector)] =
              sc
                .parallelize(idxContent, 8)


            val documentnum = qidCwidContent.length

            // Set LDA parameters

            val lda = new LDA().setK(numTopics).setMaxIterations(10)
            val ldaModel = lda.run(idxDoces)

            ldaModel
              .topicDistributions
              .map { case (cwididx, topicvector) => idxCwid(cwididx) -> topicvector.toArray }
              .map { case (cwid, topicarray) =>
                topicarray
                  .indices.map(idx => idx + ":" + "%.4E".format(topicarray(idx)))
                  .+:(cwid).mkString(" ")
              }
              .repartition(1)
              .saveAsTextFile(outdir + "/" + qid)

            println("Finished " + qid + " with " + idxCwid.size + " documents")
          }
        )
    }

    // qid -> cwid -> Array[subtopic]
    def readDiversityQrel(sc : SparkContext, qrelf: String, parNumber: Int=128): Map[Int, Set[String]]={

      sc
        .textFile(qrelf, parNumber)
        .map(line => line.split(" "))
        .filter(cols => cols.length == 4 && cols(3).toInt > 0 && cols(0).toInt > 100)
        .map{cols => cols(0).toInt -> cols(2)}
        .aggregateByKey[Set[String]](Set.empty[String])((s, p)=>s+p, (s1, s2)=>s1++s2)
        .collectAsMap()
        .toMap

    }
  }
