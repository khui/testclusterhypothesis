package de.mpii.docsimilarity.spark.eval.adhoc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by khui on 01/11/15.
  */
object EvalDocTriple {
  private var docvecdir:String = ""
  private var doctripledir: String = ""
  private var outfile = ""
  private var parNum = 1024
  private var expname:String = "vectorize doc"

  private def parse(args : List[String]) : Unit = args match {
    case ("-v") :: dir :: tail =>
      docvecdir = dir
      parse(tail)
    case ("-t") :: dir :: tail =>
      doctripledir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outfile = file
      parse(tail)
    case ("-n") :: exp :: tail =>
      expname = exp
      parse(tail)
    case ("-c") :: parn :: tail =>
      parNum = parn.toInt
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
      .setAppName("Eval on doc triple similarity benchmark " + expname)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(
        Array(classOf[String],
          classOf[(String, String)],
          classOf[(Int, String)],
          classOf[(String, Double)]))
    val sc: SparkContext = new SparkContext(conf)
    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)


    var similarityMap: Map[String, Double] = null


    val resAcc: Accumulable[ArrayBuffer[ResultTuple], ResultTuple] =
      sc.accumulable[ArrayBuffer[ResultTuple], ResultTuple](ArrayBuffer.empty[ResultTuple])

    val pool = java.util.concurrent.Executors.newFixedThreadPool(24)

    for (qid <- 101 to 300) {

      pool.execute(
        new Runnable{
          override def run(): Unit ={
            val doctripleevalfile = doctripledir + "/" + qid + "/part*"

            if (docvecdir.length > 0) {
              val docvec2eval = docvecdir + "/" + qid + "*"
              val broadcastSimiMap = docvec2Similarity(sc, docvec2eval, qid)
              val (totalnum, correctnum, nonexist) = simlarityEval(sc, doctripleevalfile, broadcastSimiMap.value, qid)
              val evalres = writeEval(qid, totalnum, correctnum, nonexist)
              resAcc.add(evalres)
              broadcastSimiMap.destroy()
            } else {
              new Throwable("Both docvecdir and simidir are unset")

            }

          }
        }


      )
    }
    pool.shutdown()

    while(!pool.isTerminated){

    }

    val out = fs.create(new Path(outfile))
    val correctR = ArrayBuffer[Double]()
    val missingR = ArrayBuffer[Double]()

    val resArray  = resAcc.localValue.toArray

    println("Finished in total: " + resArray.length + " " + resAcc.value.size)

    resArray
      .sortBy(_._1)
      .foreach(
      res =>
        {
          if(!res._5.isNaN) {
            correctR.append(res._5)
          }
          if(!res._6.isNaN) {
            missingR.append(res._6)
          }
          out.writeChars(
          List(res._1,
            res._2,
            res._3,
            res._4,
            "%#.5f".format(res._5),
            "%#.5f".format(res._6)).mkString(" ") + "\n")

        }


    )
    out.writeChars(List(0, resArray.length,"%#.5f".format(correctR.sum / correctR.length),"%#.5f".format(missingR.sum / missingR.length)).mkString(" "))
    out.close()
  }



  type ResultTuple = (Int, Int, Int, Int, Double, Double)

  /**
    *
    * @param sc
    * @param docvectorfile
    * @return
    */
  private def docvec2Similarity(sc : SparkContext, docvectorfile : String, qid : Int = 0) : Broadcast[Map[String, Double]] = {
    val cwidVector =
      sc.textFile(docvectorfile, parNum)
        .map(line => line.split(" "))
        // at least contain one nonzero vector item
        .filter(cols => cols.length > 1)
        // (cwid, array[tid:val])
        .map(cols => (cols(0), for (i <- (1 until cols.length).toArray) yield cols(i)))
        .zipWithIndex()

    println("Read vector for query " + qid + ": " + cwidVector.count())

    val cwidcwidSimilarity: Map[String, Double] = cwidVector
      .cartesian(cwidVector)
      .filter{case(x,y) => x._2 > y._2}
      .map{case(x,y) =>(cwidPairKey(x._1._1, y._1._1),cosineSimilarity(x._1._2, y._1._2))}
      .collectAsMap()
      .toMap
    println("Construct similarity map with " + cwidcwidSimilarity.size + " entries")
    sc.broadcast(cwidcwidSimilarity)
  }

  /**
    *
    * @param sc
    * @param tripleFile
    * @param similarityMap
    * @return
    */
  private def simlarityEval (sc : SparkContext, tripleFile : String, similarityMap : Map[String, Double], qid:Int=0) : (Int, Int, Int) = {
    val tripleeval: RDD[Array[String]] = sc.textFile(tripleFile, parNum)
      .map(line => line.split(" "))
      .filter(cols => cols.length >= 6)
      // (rel, rel, non-rel)
      .filter(cols => cols(3).toDouble > 0 && cols(4).toDouble > 0 && cols(5).toDouble <= 0)
      .map(cols => Array(cwidPairKey(cols(0), cols(1)), cwidPairKey(cols(0), cols(2)), cwidPairKey(cols(1), cols(2))))
      .cache()

    val missing: Array[Array[String]] =
    tripleeval
      .filter(pairlist => !(similarityMap.contains(pairlist.head) && similarityMap.contains(pairlist(1)) && similarityMap.contains(pairlist(2))))
      .take(3)

    println("missing sample:" + missing.map(v3 => Array(qid, v3.head, v3(1), v3(2)).mkString(" ")).foreach(println))

    val nonexist: Int = tripleeval
      .filter(pairlist => !(similarityMap.contains(pairlist.head) && similarityMap.contains(pairlist(1)) && similarityMap.contains(pairlist(2))))
      .count()
      .toInt

    val similist = tripleeval
      .filter(pairlist => similarityMap.contains(pairlist.head) && similarityMap.contains(pairlist(1)) && similarityMap.contains(pairlist(2)))
      .map(pairlist =>(similarityMap.get(pairlist.head).get,similarityMap.get(pairlist(1)).get, similarityMap.get(pairlist(2)).get))
      .cache()

    val totalnum : Int = similist.count().toInt

    val correctnum: Int = similist.filter(sim => sim._1 > Math.max(sim._2, sim._3)).count().toInt
    similist.unpersist()
    tripleeval.unpersist()
    (totalnum, correctnum, nonexist)
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





  private def docvecEval (sc : SparkContext, tripleFile : String, docvecMap : Map[String, Array[String]]) : (Int, Int, Int) = {
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
  }

  private def cwidPairKey(cwid1 : String, cwid2 : String) : String = {
    Array(cwid1, cwid2).sorted.mkString("-")
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
}
