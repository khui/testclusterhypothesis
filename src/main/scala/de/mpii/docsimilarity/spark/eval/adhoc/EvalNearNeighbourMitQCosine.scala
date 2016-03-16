package de.mpii.docsimilarity.spark.eval.adhoc

import de.mpii.docsimilarity.mr.utils.{IRScorer, GlobalConstants}
import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import de.mpii.docsimilarity.spark.doc2vec.tweimpl.TermTopicality._
import de.mpii.docsimilarity.spark.eval.EvalIOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.immutable.{Iterable, IndexedSeq}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by khui on 01/11/15.
  */
object EvalNearNeighbourMitQCosine {
  private var docvecdir:String = ""
  private var outdir = ""
  private var qreldir = ""
  private var queryfile = ""
  private var parNum = 1024
  private var expname:String = "vectorize doc"


  private def parse(args : List[String]) : Unit = args match {
    case ("-v") :: dir :: tail =>
      docvecdir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outdir = file
      parse(tail)
    case ("-c") :: coreNum :: tail =>
      parNum = coreNum.toInt
      parse(tail)
    case ("-n") :: exp :: tail =>
      expname = exp
      parse(tail)
    case ("-t") :: file :: tail =>
      queryfile = file
      parse(tail)
    case ("-q") :: qrel :: tail =>
      qreldir = qrel
      parse(tail)
    case(Nil) =>
  }

  implicit def arrAccum[A]= new AccumulableParam[ArrayBuffer[A], A] {
    def addInPlace(t1: ArrayBuffer[A], t2: ArrayBuffer[A]): ArrayBuffer[A] = {
      t1 ++= t2
      t1
    }

    def addAccumulator(t1: ArrayBuffer[A], t2: A): ArrayBuffer[A] = {
      t1 += t2
      t1
    }

    def zero(t1: ArrayBuffer[A]): ArrayBuffer[A] = {
      new ArrayBuffer[A]().++=(t1)
    }
  }

  def main(args : Array[String]): Unit = {
    parse(args.toList)
    val conf: SparkConf = new SparkConf()
      .setAppName("Eval on near-neighour benchmark (query sensitive) " + expname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)]))
    val sc: SparkContext = new SparkContext(conf)
    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    val qidCwidLabel: Map[Int, Map[String, Int]] = EvalIOUtils.readAdhocQrel(sc,qreldir + "/qrels.adhoc.*", parNum)

    println("Finished reading in qrels: " + qidCwidLabel.size)

    val bcqrel = sc.broadcast(qidCwidLabel)

    val resAcc: Accumulable[ArrayBuffer[ResultTuple], ResultTuple] =
      sc.accumulable[ArrayBuffer[ResultTuple], ResultTuple](ArrayBuffer.empty[ResultTuple])

    val yearTermidxdfcf: Map[String, Map[String, (Int, Int, Int)]] =
      Array("cw09", "cw12")
        .par
        .map(year => year -> termIdxDfCf(sc, year, parNum))
        .toSeq
        .seq
        .toMap

    val termTermid: Map[String, Map[String, Int]] = yearTermidxdfcf
      .map{case(y, m) => y -> m.map(v => v._1 -> v._2._1)}

    val queries: Map[Int, Array[String]] = EvalIOUtils.readQueries(queryfile)

    val doclen: Map[String, Int] =
      Array("cw09", "cw12")
        .map(year => (year, year match {
          case "cw09" => GlobalConstants.DOCFREQUENCY_CW09
          case "cw12" => GlobalConstants.DOCFREQUENCY_CW12
        }))
        .map(v => v._1 -> v._2)
        .toMap


    val psf: VectorizeDoc = new VectorizeDoc()

    val qidTidnIdf: Map[Int, Map[Int, Double]] =
      queries
        .map { case (qid, terms) =>
          val (year, cardinality) = psf.qid2year(qid)
          val tidIdf =
            terms
              .filter(term => yearTermidxdfcf(year).contains(term))
              .map(term => yearTermidxdfcf(year)(term))
              .map(v => v._1 -> v._2)
              .map(v => v._1 -> IRScorer.idf(v._2, doclen(year)))
              .toMap
          val norm: Double = Math.sqrt(tidIdf.map(v => v._2 * v._2).sum)
          val tidnIdf: Map[Int, Double] = tidIdf.map(v => v._1 -> v._2 / norm)
          qid -> tidnIdf
        }



    val topks = Array(5, 10, 15, 20, 25, 30)

    (101 to 300)
      .par
      .foreach(
        qid => {
          println("Processing " + qid)
          val docvec2eval = docvecdir + "/" + qid
          val cwidTopSimilarDocs = topkSimilarDocs(sc, qid, docvec2eval, bcqrel.value(qid), qidTidnIdf(qid), topks.max, parNum)
          val topkAvgRelratio = simlarityEval(cwidTopSimilarDocs, bcqrel.value(qid), topks)
          resAcc.add((qid, topkAvgRelratio))
          println("Finished " + topkAvgRelratio.toSeq.sortBy(_._1).map(kv => kv._1+":"+"%#.4f".format(kv._2)).+:(qid).mkString("\t"))
        }
      )

    val resArray = resAcc.localValue.toArray

    println("Finished in total: " + resArray.length + " " + resAcc.value.size)

    topks
      .toSeq
      .par
      .foreach {
        topk =>
          val out = fs.create(new Path(outdir + "/" + topk))
          val correctR = ArrayBuffer[Double]()
          resArray
            .sortBy(_._1)
            .foreach(
              topkSimilarity => {
                if (topkSimilarity._2.contains(topk)) {
                  val relRatio: Double = topkSimilarity._2.get(topk).get
                  if (!relRatio.isNaN) {
                    correctR.append(relRatio)
                  }
                  out.writeChars(
                    List(topkSimilarity._1,
                      "%#.6f".format(relRatio), "\n").mkString(" ")
                  )
                }
              }
            )
          out.writeChars(List(0, resArray.length, "%#.6f".format(correctR.sum / correctR.length), "\n").mkString(" "))
          out.close()
      }
  }

  type ResultTuple = (Int, Map[Int, Double])

  /**
    * For docs in cwid2consider, compute similarity w.r.t. all other docs,
    * only retaining the maxTopk docs for each row.
    *
    *
    * @param sc
    * @param docvectorfile
    * @param cwidLabel
    * @param maxTopK
    * @param parNum
    * @return
    */
  private def topkSimilarDocs(sc : SparkContext, qid: Int, docvectorfile : String, cwidLabel: Map[String, Int], tidnIdf: Map[Int, Double], maxTopK: Int=50, parNum:Int=1024) :  RDD[(String, Array[(String, Double)])] = {
    val cwidVector: RDD[((String, Array[String]), Long)] =
      sc.textFile(docvectorfile + "*", parNum)
        .map(line => line.split(" "))
        // at least contain one nonzero vector item
        .filter(cols => cols.length > 1)
        // (cwid, array[tid:val])
        .map(cols => (cols(0), for (i <- (1 until cols.length).toArray) yield cols(i)))
        .filter(a => cwidLabel.contains(a._1))
        .zipWithIndex()

    val cwidSortedCwids: RDD[(String, Array[(String, Double)])] = cwidVector
      .cartesian(cwidVector)
      // we only consider document similarity w.r.t. documents in the cwid2consider, e.g., relevant docs
      .filter{case(x,y) => x._2 > y._2 && (cwidLabel(x._1._1) > 0 || cwidLabel(y._1._1) > 0)}
      .map{case(x,y) =>((x._1._1,y._1._1), qcosineSimilarity(x._1._2, y._1._2, tidnIdf))}
      .flatMap{case(cwids, similarity) => Array(cwids._1->(cwids._2, similarity), cwids._2->(cwids._1, similarity))}
      // we only need the row for doc for relevant docs
      .filter(t2 =>  cwidLabel(t2._1) > 0)
      .aggregateByKey[Array[(String, Double)]](Array.empty[(String, Double)])((a, t2) => a.+:(t2), (a1, a2)=>a1.++(a2))
      .map(t2 => t2._1 -> t2._2.sortBy(e2 => -e2._2).slice(0, maxTopK))
    println("Compute topkSimilarDocs successfully for " + qid)
    cwidSortedCwids
  }

  /**
    *
    * @param topkSimilarDocs
    * @param cwidLabel
    * @param topks
    * @return
    */
  private def simlarityEval (topkSimilarDocs : RDD[(String, Array[(String, Double)])], cwidLabel: Map[String, Int], topks : Array[Int]): Map[Int, Double]= {
    topkSimilarDocs
      .flatMap(t2 =>
      {
        val labels: IndexedSeq[Int] =
        (0 until Math.min(topks.max, t2._2.length))
          .map(idx => if (cwidLabel(t2._2(idx)._1) > 0) 1 else 0)
        val relRatio: Array[(Int, Double)] =
        topks
          .map(topk => topk -> labels.slice(0, topk).sum / topk.toDouble)
        relRatio
      }
      )
    .aggregateByKey[Array[Double]](Array.empty[Double])((a, t) => a.+:(t), (a1, a2) => a1.++(a2))
    .map(t2 => t2._1 -> t2._2.sum / t2._2.length.toDouble)
    .collectAsMap()
    .toMap
  }

  private def writeEval(qid:Int, totalnum:Int, correctnum:Int, nonexist:Int) =
  {
    val correctRate: Double = correctnum.toDouble / totalnum
    val nonexistRate: Double = nonexist.toDouble / (nonexist + totalnum)

    val evalres: (Int, Int, Int, Int, Double, Double) =
      (qid, totalnum, correctnum, nonexist, correctRate, nonexistRate)



    println("Finished processing " + qid + " " + evalres)
    evalres
  }



/*  private def docvec2Similarity(sc : SparkContext, docvectorfile : String, dim : Int ) : Broadcast[Map[String, Double]] = {
    val cwidVector: RDD[((String, Array[String]), Long)] =
      sc.textFile(docvectorfile + "*", 100)
        .map(line => line.split(" "))
        // at least contain one nonzero vector item
        .filter(cols => cols.length > 1)
        .map(cols => (cols(0), for (i <- (1 until cols.length).toArray) yield cols(i)))
        .zipWithIndex()

    val cwidcwidSimilarity: Map[String, Double] = cwidVector
      .cartesian(cwidVector)
      .filter{case(x,y) => x._2 > y._2}
      .map{case(x,y) =>( cwidPairKey(x._1._1, y._1._1),cosineSimilarity(x._1._2, y._1._2))}
      .collectAsMap()
      .toMap
    println("Construct similarity map with " + cwidcwidSimilarity.size + " entries")
    sc.broadcast(cwidcwidSimilarity)
  }*/



  private def readDocvec(sc : SparkContext, docvectorfile : String) : Broadcast[Map[String,Array[String]]] = {
    val cwidVector = sc.textFile(docvectorfile + "*", 100)
      .map(line => line.split(" "))
      // at least contain one nonzero vector item
      .filter(cols => cols.length > 1)
      .map(cols => (cols(0), for (i <- (1 until cols.length).toArray) yield cols(i)))
      .collectAsMap()
      .toMap
    sc.broadcast(cwidVector)
  }

  private def readDocSimilarity(sc : SparkContext, docsimiFile : String) : Map[String, Double] = {
    // file format: qid, cwid1, cwid2, similarity
    sc.textFile(docsimiFile + "*", 100)
      .map(line => line.split(" "))
      .filter(cols => cols.length == 5)
      .map(cols => (cwidPairKey(cols(1), cols(2)), cols(4).toDouble))
      .collectAsMap()
      .toMap
  }





/*  private def docvecEval (sc : SparkContext, tripleFile : String, docvecMap : Map[String, Array[String]]) : (Int, Int, Int) = {
    val tripleeval: RDD[(String, String, String)] = sc.textFile(tripleFile, 5)
      .map(line => line.split(" "))
      .filter(cols => cols.length == 6)
      // (rel, rel, non-rel)
      .filter(cols => cols(3).toDouble > 0 && cols(4).toDouble > 0 && cols(5).toDouble <= 0)
      .map(cols => (cols(0), cols(1), cols(2)))
      .cache()


    val nonexist: Int = tripleeval
      .filter(pairlist => !(docvecMap.contains(pairlist._1) && docvecMap.contains(pairlist._2) && docvecMap.contains(pairlist._3)))
      .count()
      .toInt

    val totalnum : Int = tripleeval.count().toInt

    val similist = tripleeval
      .filter(pairlist => docvecMap.contains(pairlist._1) && docvecMap.contains(pairlist._2) && docvecMap.contains(pairlist._3))
      .map{
        triple =>
          (
            cosineSimilarity(docvecMap.get(triple._1).get, docvecMap.get(triple._2).get),
            cosineSimilarity(docvecMap.get(triple._1).get, docvecMap.get(triple._3).get),
            cosineSimilarity(docvecMap.get(triple._2).get, docvecMap.get(triple._3).get)
            )

      }



    val correctnum: Int = similist.filter(sim => sim._1 > Math.max(sim._2, sim._3)).count().toInt

    tripleeval.unpersist()
    (totalnum, correctnum, nonexist)
  }*/

  private def cwidPairKey(cwid1 : String, cwid2 : String) : String = {
    List(cwid1, cwid2).sorted.mkString(" ")
  }

  /**
    * Compute cosine similarity
    *
    * @param vec1 : Array["idx:value" : String]
    * @param vec2 : Array["idx:value" : String]
    * @return
    */
  private def cosineSimilarity(vec1: Array[String], vec2: Array[String]) : Double = {
    val xvec: Map[Int, (Double, Double)] =
      (for(i <- vec1.indices.toArray) yield vec1(i).split(":"))
        .map(kv => kv(0).toInt->kv(1).toDouble)
        .map{case(tid, v) => tid -> (v, v * v)}
        .toMap
    val yvec: Map[Int, (Double, Double)] =
      (for(i <- vec2.indices.toArray) yield vec2(i).split(":"))
        .map(kv => kv(0).toInt -> kv(1).toDouble)
        .map{case(tid, v) => tid -> (v, v * v)}
        .toMap
    val tids: Set[Int] = xvec.keySet.intersect(yvec.keySet)
    val xnorm: Double  = (for(x <- xvec) yield x._2._2).sum
    val ynorm: Double = (for(y <- yvec) yield y._2._2).sum
    val dotproduct= tids
      .map{tid => xvec.get(tid).get._1 * yvec.get(tid).get._1}
      .sum
    dotproduct / Math.sqrt(xnorm * ynorm)
  }

  private def qcosineSimilarity(vec1: Array[String], vec2: Array[String], query: Map[Int, Double]) : Double = {
    val xvec: Map[Int, (Double, Double)] =
      (for(i <- vec1.indices.toArray) yield vec1(i).split(":"))
        .map(kv => kv(0).toInt->kv(1).toDouble)
        .map{case(tid, v) => tid -> (v, v * v)}
        .toMap
    val yvec: Map[Int, (Double, Double)] =
      (for(i <- vec2.indices.toArray) yield vec2(i).split(":"))
        .map(kv => kv(0).toInt -> kv(1).toDouble)
        .map{case(tid, v) => tid -> (v, v * v)}
        .toMap
    val tids: Set[Int] = xvec.keySet.intersect(yvec.keySet)
    val xnorm: Double  = (for(x <- xvec) yield x._2._2).sum
    val ynorm: Double = (for(y <- yvec) yield y._2._2).sum
    val dotproduct= tids
      .map{tid => xvec.get(tid).get._1 * yvec.get(tid).get._1}
      .sum
    val docsimilarity = dotproduct / Math.sqrt(xnorm * ynorm)
    // query similarity
    val davg: Iterable[(Double, Double)] =
      query
        .map{case(k, v) =>
          (k, (xvec.getOrElse(k, (0d,0d))._1 + yvec.getOrElse(k, (0d,0d))._1) / 2d, v)
        }
        .map{case(k, v, qv) => (v * qv, v * v)}
    val dotproductDavgq: Double = davg.map(v => v._1).sum
    val normdavg: Double = Math.sqrt(davg.map(v => v._2).sum)
    val davgQuerySimilarity = dotproductDavgq / normdavg
    docsimilarity * davgQuerySimilarity
  }



}
