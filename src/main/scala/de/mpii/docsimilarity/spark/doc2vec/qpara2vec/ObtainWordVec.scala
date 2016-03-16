package de.mpii.docsimilarity.spark.doc2vec.qpara2vec


import java.io._
import java.util

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.utils.{CleanContentTxt, GlobalConstants, StopWordsFilter}
import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import de.mpii.docsimilarity.spark.eval.EvalIOUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.models.embeddings.inmemory.InMemoryLookupTable
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.word2vec.Word2Vec.Builder
import org.deeplearning4j.models.word2vec.wordstore.VocabCache
import org.deeplearning4j.models.word2vec.wordstore.inmemory.InMemoryLookupCache
import org.deeplearning4j.models.word2vec.{VocabWord, Word2Vec}
import org.deeplearning4j.text.sentenceiterator.{SentenceIterator, SentencePreProcessor}
import org.jsoup.Jsoup
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.ops.transforms.Transforms

import scala.collection.JavaConversions._

/**
  * Created by khui on 20/12/15.
  * since the spark fails to call the native blas,
  * this code is not used and the python version is
  * used instead.
  */
object ObtainWordVec {

  private var seqdocdir: String = ""
  private var outdir = ""
  private var qreldir: String = ""
  private var queryfile: String = ""
  private var parNum = 1024
  private var numTopics = 20
  private var cwyear: String = "cw4y"
  private var googlew2v: String = ""


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
    case ("-j") :: qrel :: tail =>
      qreldir = qrel
      parse(tail)
    case ("-q") :: file :: tail =>
      queryfile = file
      parse(tail)
    case ("-g") :: path :: tail =>
      googlew2v = path
      parse(tail)
    case ("-y") :: year :: tail =>
      cwyear = year
      parse(tail)
    case (Nil) =>
  }




  def main(args: Array[String]): Unit = {
    parse(args.toList)
    val conf: SparkConf = new SparkConf()
      .setAppName("load  google news w2v")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)]))
    val sc: SparkContext = new SparkContext(conf)


/*    try {
      System.loadLibrary("crypt"); // used for tests. This library in classpath only
    } catch (ex : UnsatisfiedLinkError ) {
      try {
        NativeUtils.loadLibraryFromJar("/natives/crypt.dll"); // during runtime. .DLL within .JAR
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
    }*/



    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)
    if (fs.exists(new Path(outdir))) {
      println(outdir + " is being removed")
      fs.delete(new Path(outdir), true)
    }

    val pathList: Array[String] = fs
      .listStatus(new Path(seqdocdir))
      .filter(f => !f.getPath.getName.startsWith("_"))
      .map(f => f.getPath.toString)

    val years = cwyear match {
      case "cw09" => Array("cw09")
      case "cw12" => Array("cw12")
      case "cw4y" => Array("cw09", "cw12")
    }

    val queries: Map[Int, Array[String]] =
      years
        .flatMap(y => EvalIOUtils.readQueries(queryfile).toSeq)
        .toMap
    println("Read in queries: " + queries.size)


    val psf: VectorizeDoc = new VectorizeDoc()
    val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)
    val qids = Array(101, 201, 202)
      //(101 to 300).toSet

    val stopWords = StopWordsFilter.STOPWORDS.toList
/*
    val invertedIndex: InvertedIndex = new LuceneInvertedIndex
    .Builder()
      .stopWords(stopWords)
      .indexDir(new File("/scratch/GW/pool0/khui/tmp/word-index"))
      .build()
*/

    val batchSize = 1000
    val iterations = 30
    val layerSize = 300



    val w2vBuilder: Builder = new Builder()
      .batchSize(batchSize) //# words per minibatch.
      .sampling(1e-5) // negative sampling. drops words out
      .minWordFrequency(2) //
      .useAdaGrad(false) //
      .layerSize(layerSize) // word feature vector size
      .iterations(iterations) // # iterations to train
      .learningRate(0.025) //
      .minLearningRate(1e-2) // learning rate decays wrt # words. floor learning
      .negativeSample(10) // sample size 10 words
      .stopWords(stopWords)
      // .iterate(new SentenceIte(docs)) //
      //.index(invertedIndex)
      //.vocabCache(loadedW2v.vocab())
      //.lookupTable(loadedW2v.lookupTable())

    loadGoogleNewsW2v(sc, new Path(googlew2v), w2vBuilder)


    /*if (word2vecModelOpt.isEmpty) {
      print(googlew2v + " is loaded as null.")
      System.exit(1)
    }*/

    //val word2vecModel: Word2Vec = w2vBuilder.build()
      //word2vecModelOpt.get

   // print("loaded " + word2vecModel.vocab().numWords())



    val documents2train: Array[String] = pathList
      .par
      .map(p => (p, psf.path2qid(p)))
      .filter(t2 => qids.contains(t2._2))
      .map(
        pathQid => {

          val path = pathQid._1
          val qid = pathQid._2
          val (year, collen, cwrecord) = psf.qid2yearrecord(qid)

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
              .filter(v => v._3 != null)
              .map { case (q, cwid, htmlDoc) => (q, cwid, cleanpipeline.cleanTxtStr(htmlDoc, cwid, true)) }
              .filter(v => v._3 != null)
          // only for diversity
          // .filter(v => qidRelCwids(qid).contains(v._2))



          val docs: Array[String] = qidCwidContent.map(v => v._3)
          println("Finished reading in " + docs.length + " for query " + qid)
          docs
        }
      )
      .flatMap(v => v)
      .toArray

    val docs = new SentenceIte(documents2train)

    val word2vecModel = w2vBuilder.build()

    val termnum = documents2train.flatMap(v => v.split(" ")).distinct.length

    println("Before training 0 "+word2vecModel.vocab().numWords())

    word2vecModel.setSentenceIter(docs)

    word2vecModel.fit()

    println("After training "+word2vecModel.vocab().numWords() + " actual term number " + termnum)



    queries
      .foreach{
        case(qid, terms) =>
          val topkterms = word2vecModel.wordsNearest(terms.toList, new util.ArrayList(), 20)
          println(qid + " " + topkterms)
      }

    println("Start writing to file")

    WordVectorSerializer.writeWordVectors(word2vecModel, outdir + "/dump-google-101-300.txt")


    println("Save to file for queries with " + word2vecModel.vocab().numWords() + " terms")




  }




  /**
    * Modified from deeplearning4j
    * org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
    * Read a binary word2vec file.
    *
    * @param modelFile
     * the File to read
    * @param linebreaks
     * if true, the reader expects each word/vector to be in a separate line, terminated
    *  by a line break
    * @return a { @link Word2Vec model}
    * @throws NumberFormatException
    * @throws IOException
    * @throws FileNotFoundException
    */
  @throws(classOf[NumberFormatException])
  @throws(classOf[IOException])
  private def loadGoogleNewsW2v(sc: SparkContext, modelFile: Path, w2vBuilder: Builder, linebreaks: Boolean = false): Option[Word2Vec]= {
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)
    var lookupTable: InMemoryLookupTable = null
    var cache: VocabCache = null
    var syn0: INDArray = null
    val bis: BufferedInputStream = new BufferedInputStream(fs.open(modelFile))
    val dis: DataInputStream = new DataInputStream(bis)
    try {
      val words = 500
       val a = WordVectorSerializer.readString(dis).toInt

      val size = WordVectorSerializer.readString(dis).toInt

      syn0 = Nd4j.create(words, size)
      cache = new InMemoryLookupCache(false)
      lookupTable = new InMemoryLookupTable.Builder().cache(cache).vectorLength(size).build.asInstanceOf[InMemoryLookupTable]

      (0 until words)
        .foreach { wordidx =>
          val word = WordVectorSerializer.readString(dis)
          val vector: Array[Float] =
            (0 until size)
              .map(item => WordVectorSerializer.readFloat(dis))
              .toArray
          val vec: INDArray = Transforms.unitVec(Nd4j.create(vector))
          syn0.putRow(wordidx, vec)
          cache.addWordToIndex(cache.numWords, word)
          cache.addToken(new VocabWord(1, word))
          cache.putVocabWord(word)
        }
     // val ret: Word2Vec = new Word2Vec
      lookupTable.setSyn0(syn0)
      w2vBuilder.lookupTable(lookupTable)
      w2vBuilder.vocabCache(cache)
      //lookupTable.setVocab(cache)
      //word2vecModel.setVocab(cache)
      //word2vecModel.setLookupTable(lookupTable)
      println("Loaded " + cache.numWords() + " terms loaded.")
      Some(w2vBuilder.build())
    } finally {
      if (dis != null) dis.close()
      if (bis != null) bis.close()
      None
    }
  }

  private class SentenceIte(docs: Array[String]) extends SentenceIterator {

    protected var preProcessor: SentencePreProcessor = null

    protected var sentences: Iterator[String] = docs.toIterator

    override def finish(): Unit = sentences.hasNext

    override def nextSentence(): String = sentences.next()

    override def hasNext: Boolean = sentences.hasNext

    override def getPreProcessor: SentencePreProcessor = preProcessor

    override def setPreProcessor(preProcessor: SentencePreProcessor): Unit = {
      this.preProcessor = preProcessor
    }

    override def reset() = {
      sentences = docs.toArray.toIterator
    }
  }


}
