package de.mpii.docsimilarity.spark.eval.adhoc

import de.mpii.docsimilarity.MaxRepSelection
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.immutable.HashMap

/**
  * Created by khui on 16/11/15.
  */
class SplitTrainTestData {

  def computeDocSimilarity(docs: RDD[(String, Vector)]):
  (Map[Long, String], Array[((Long, Long), Double)]) = {

    val docVectors: RDD[((String, Vector), Long)] = docs
      .zipWithIndex()
      .cache()

    val idxCwid: Map[Long, String] = docVectors
      .map(t2 => t2._2 -> t2._1._1)
      .collectAsMap()



    val docTerms: RDD[IndexedRow] = docVectors
      .map(t2 => new IndexedRow(t2._2, t2._1._2))


    val docdocSimilarity: Array[((Long, Long), Double)] = new IndexedRowMatrix(docTerms)
      .toCoordinateMatrix()
      .transpose()
      .toRowMatrix()
      .columnSimilarities()
      .entries
      .collect()
      .map(entry => (entry.i, entry.j) -> entry.value)

    (idxCwid, docdocSimilarity)
  }


  def maxRep(labeledDocs: RDD[(String, LabeledPoint)], trainPercent:Double=0.2, similarityThreshold : Double = 0.8)
  :((RDD[String], RDD[LabeledPoint]),(RDD[String], RDD[LabeledPoint]), Int, Int)= {
    labeledDocs.cache()
    val cwidVector: RDD[(String, Vector)] = labeledDocs
      .map { case (cwid, lp) => (cwid, lp.features) }
    val idxcwidSimilarity: (Map[Long, String], Array[((Long, Long), Double)])
    = computeDocSimilarity(cwidVector)

    val docnum = idxcwidSimilarity._1.size
    val idxCwidDict = HashMap[Int, String]()
    var idx = 0
    val similarityMatrix = Array.ofDim[Double](docnum, docnum)
    idxcwidSimilarity
      ._2
      .foreach(
        t2 => {
          val indx = t2._1._1.toInt
          val indy = t2._1._2.toInt
          val similarity = t2._2
          similarityMatrix(indx)(indy) = similarity
          similarityMatrix(indy)(indx) = similarity
        }
      )
    val selector = new MaxRepSelection(similarityMatrix, similarityThreshold)
    val docnum2select = (docnum * trainPercent).toInt
    val selectedDocs: Set[String] = selector
      .selectMaxRep(docnum2select)
      .map(idx => idxcwidSimilarity._1.get(idx).get)
      .toSet
    System.out.println("Selected " + selectedDocs.size + " documents for training")
    val training = labeledDocs
      .filter(t2 => selectedDocs.contains(t2._1)).cache()
    val test = labeledDocs
      .filter(t2 => !selectedDocs.contains(t2._1)).cache()

    labeledDocs.unpersist()
    val trainCwids: RDD[String] = training.map(_._1)
    val trainVec: RDD[LabeledPoint] = training.map(_._2)
    val testCwids = test.map(_._1)
    val testVec = test.map(_._2)
    ((trainCwids, trainVec), (testCwids, testVec), docnum, selectedDocs.size)

  }

  def rndSplit(labeledDocs: RDD[(String, LabeledPoint)], trainPercent:Double=0.4)
  :((RDD[String], RDD[LabeledPoint]),(RDD[String], RDD[LabeledPoint]), Int, Int)  = {
    labeledDocs.cache()
    val splits = labeledDocs.randomSplit(Array(trainPercent, 1 - trainPercent), seed = 11L)
    val training: RDD[(String, LabeledPoint)] = splits(0).cache()
    val test: RDD[(String, LabeledPoint)] = splits(1).cache()
    val docnum = labeledDocs.count()
    val trainnum = training.count()
    val trainCwids: RDD[String] = training.map(_._1)
    val trainVec: RDD[LabeledPoint] = training.map(_._2)
    val testCwids = test.map(_._1)
    val testVec = test.map(_._2)
    labeledDocs.unpersist()
    training.unpersist()
    test.unpersist()
    ((trainCwids, trainVec), (testCwids, testVec), docnum.toInt, trainnum.toInt)
  }


}
