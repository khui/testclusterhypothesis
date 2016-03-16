package de.mpii.docsimilarity.spark.eval

import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}

import de.mpii.docsimilarity.mr.utils.CleanContentTxt
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.w3c.dom.{Document, Element, Node, NodeList}

/**
  * Created by khui on 16/11/15.
  */
object EvalIOUtils {

  /**
    * Input format:
    * each line represents a document
    * cwid idx:val idx:val ...
    * @param sc
    * @param featuref
    * @param parNumber
    * @return
    */
  def readSparseCWDocs(sc : SparkContext, featuref: String, dim:Int, parNumber:Int=10) : RDD[(String, Vector)] = {
    sc
      .textFile(featuref, parNumber)
      .map(line => line.split(" "))
      .filter(cols => cols.length > 1)
      .map { cols => {
        val idxvals = for (i <- 1 until cols.length)
          yield cols(i).split(":")
        val features: Seq[(Int, Double)] = idxvals
          .filter(idxval => idxval.length == 2)
          .map(f => (f(0).toInt, f(1).toDouble))
        (cols(0), Vectors.sparse(dim, features))
      }
      }
  }

  def readAdhocQrel(sc : SparkContext, qrelf: String, parNumber: Int=128): Map[Int, Map[String, Int]]={
    sc
      .textFile(qrelf, parNumber)
      .map(line => line.split(" "))
      .filter(cols => cols.length == 4)
      .map{cols => cols(0).toInt -> (cols(2) -> (if (cols(3).toInt > 0) 1 else 0))}
      .aggregateByKey[Seq[(String,Int)]](Seq.empty[(String,Int)])((s, t2) => s.+:(t2), (s1, s2) => s1.++(s2))
      .map{case(qid, seq) => qid -> seq.toMap}
      .collectAsMap()
      .toMap
  }

  /**
    *
    * @param queryfile path in hdfs
    * @return qid-list of query terms
    */
  def readQueries(queryfile: String, termTermid: Map[String, Int]): Map[Int, Array[Int]] = {
    val qidQueryTerms: mutable.Map[Int, Array[Int]] = mutable.Map[Int, Array[Int]]()
    try {
      val factory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance
      val builder: DocumentBuilder = factory.newDocumentBuilder
      val doc: Document = builder.parse(queryfile)
      val nList: NodeList = doc.getElementsByTagName("topic")
      for (idx <- 0 until nList.getLength) {
        {
          val nNode: Node = nList.item(idx)
          val eElement: Element = nNode.asInstanceOf[Element]
          val queryId: Int = eElement.getAttribute("number").toInt
          val query: String = eElement.getElementsByTagName("query").item(0).getTextContent
          val terms = CleanContentTxt
            .queryTxt(query)
            .toArray[String](Array.empty[String])
            .filter(t => termTermid.contains(t))
            .map(t => termTermid(t))
          qidQueryTerms.put(queryId, terms)
        }
      }
      println("Finished read in query files: " + qidQueryTerms.size)
    }
    catch {
      case ex: Exception => {
        System.err.println("", ex)
      }
    }
     qidQueryTerms.toMap
  }

  /**
    *
    * @param queryfile path in hdfs
    * @return qid-list of query terms
    */
  def readQueriesDes(queryfile: String, termTermid: Map[String, Int]): Map[Int, Array[Int]] = {
    val qidQueryTerms: mutable.Map[Int, Array[Int]] = mutable.Map[Int, Array[Int]]()
    try {
      val factory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance
      val builder: DocumentBuilder = factory.newDocumentBuilder
      val doc: Document = builder.parse(queryfile)
      val nList: NodeList = doc.getElementsByTagName("topic")
      for (idx <- 0 until nList.getLength) {
        {
          val nNode: Node = nList.item(idx)
          val eElement: Element = nNode.asInstanceOf[Element]
          val queryId: Int = eElement.getAttribute("number").toInt
          val query: String = eElement.getElementsByTagName("query").item(0).getTextContent
          val description: String = eElement.getElementsByTagName("description").item(0).getTextContent
          val terms: Array[Int] = CleanContentTxt
            .queryTxt(query + " " + description)
            .toArray[String](Array.empty[String])
            .filter(t => termTermid.contains(t))
            .map(t => termTermid(t))
            .distinct
          qidQueryTerms.put(queryId, terms)
        }
      }
      println("Finished read in query files: " + qidQueryTerms.size)
    }
    catch {
      case ex: Exception => {
        System.err.println("", ex)
      }
    }
    qidQueryTerms.toMap
  }

  def readQueries(queryfile: String): Map[Int, Array[String]] = {
    val qidQueryTerms: mutable.Map[Int, Array[String]] = mutable.Map[Int, Array[String]]()
    try {
      val factory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance
      val builder: DocumentBuilder = factory.newDocumentBuilder
      val doc: Document = builder.parse(queryfile)
      val nList: NodeList = doc.getElementsByTagName("topic")
      println("readQueries nList " + nList.getLength)
      for (idx <- 0 until nList.getLength) {
        {
          val nNode: Node = nList.item(idx)
          val eElement: Element = nNode.asInstanceOf[Element]
          val queryId: Int = eElement.getAttribute("number").toInt
          val query: String = eElement.getElementsByTagName("query").item(0).getTextContent
          val terms = CleanContentTxt
            .queryTxt(query)
            .toArray[String](Array.empty[String])

          qidQueryTerms.put(queryId, terms)
        }
      }
      println("Finished read in query files: " + qidQueryTerms.size)
    }
    catch {
      case ex: Exception => println(ex.getLocalizedMessage, ex)
    }
    qidQueryTerms.toMap
  }


  def createLabeledPoints(cwidVector: RDD[(String, Vector)], adhocQrel : Broadcast[Map[String,Int]]): RDD[(String, LabeledPoint)] ={
    cwidVector
      .map{case(cwid, vector) => (cwid, adhocQrel.value.getOrElse(cwid, -1), vector)}
      .filter(lv => lv._2 >= 0)
      .map(lv => lv._1 -> LabeledPoint(lv._2, lv._3))
  }



}
