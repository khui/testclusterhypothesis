package de.mpii.docsimilarity.spark.doc2vec

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util

import de.mpii.docsimilarity.mr.input.clueweb.ClueWebWarcRecord
import de.mpii.docsimilarity.mr.utils.{CleanContentTxt, GlobalConstants, IRScorer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.immutable.Iterable
import scala.collection.mutable


/**
  * Created by khui on 04/11/15.
  */
class VectorizeDoc extends java.io.Serializable{

  def docstr2TfidfSeqTidTerm(doccontent: String, collectionLen: Int, termIdxDfCf: Map[String, (Int, Int, Int)], tokenizer: Option[TokenizerFactory]=None): (Seq[(Int, Double)],Seq[(Int, String)]) = {
    val termTidTfidf = docstr2Tfidf(doccontent, collectionLen, termIdxDfCf, tokenizer)
    val termTfidf = termTidTfidf.map(t => (t._2, t._3)).toSeq
    val tidTerm = termTidTfidf.map(t => t._2->t._1).distinct.toSeq
    (termTfidf, tidTerm)
  }

  def docstr2Termids(doccontent: String, collectionLen: Int, termIdxDfCf: Map[String, (Int, Int, Int)], tokenizer: Option[TokenizerFactory]=None): Array[Int] ={
    val docterms = if (tokenizer.isEmpty) doccontent.split(" ") else tokenizer.get.create(doccontent).getTokens.toArray(Array[String]())
    val termids = docterms
      .filter(termIdxDfCf.contains)
      .map(v => termIdxDfCf(v)._1)
      .distinct
    termids
  }

  def docstr2TfidfSeq(doccontent: String, collectionLen: Int, termIdxDfCf: Map[String, (Int, Int, Int)], tokenizer: Option[TokenizerFactory]=None): Seq[(Int, Double)] = {
    val termTidTfidf: Array[(String, Int, Double)] = docstr2Tfidf(doccontent, collectionLen, termIdxDfCf, tokenizer)
    val termTfidf: Seq[(Int, Double)] = termTidTfidf.map(t => (t._2, t._3)).toSeq
    termTfidf
  }

  def docstr2TfidfVector(doccontent: String, collectionLen: Int, termIdxDfCf: Map[String, (Int, Int, Int)], tokenizer: Option[TokenizerFactory]=None): Vector = {
    val termTfidf: Seq[(Int, Double)] = docstr2TfidfSeq(doccontent, collectionLen, termIdxDfCf)
    val docVec: Vector = Vectors.sparse(collectionLen, termTfidf)
    docVec
  }



  private def docstr2Tfidf(doccontent: String, collectionLen: Int, termIdxDfCf: Map[String, (Int, Int, Int)], tokenizer: Option[TokenizerFactory]=None): Array[(String, Int, Double)] ={
    val docterms: Array[String] = if (tokenizer.isEmpty) doccontent.split(" ") else tokenizer.get.create(doccontent).getTokens.toArray(Array[String]())
    val termTidTfidf: Array[(String, Int, Double)] = docterms
      .filter(termIdxDfCf.contains)
      .map(v => (v, 1))
      .groupBy(_._1)
      .map { case (t, ite) => (t, ite.length) }
      .map { case (term, tf) => (term, termIdxDfCf.get(term).get._1, IRScorer.tfIdf(tf,
        termIdxDfCf.get(term).get._2.asInstanceOf[Double], docterms.length,
        collectionLen))
      }.toArray
    termTidTfidf
  }


  def path2qid(path: String) = {
    new Path(path).getName.split("-")(0).toInt
  }
  def qid2year(qid: Int): (String, Int) = {
    qid match {
      case x if 1 until 201 contains x => ("cw09", GlobalConstants.CARDINALITY_CW09)
      case x if 201 until 301 contains x => ("cw12", GlobalConstants.CARDINALITY_CW12)
    }
  }
  def qid2yearrecord(qid: Int): (String, Int, String) = {
    qid match {
      case x if 1 to 200 contains x => ("cw09", GlobalConstants.CARDINALITY_CW09,
        "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord")
      case x if 201 to 300 contains x => ("cw12", GlobalConstants.CARDINALITY_CW12,
        "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord")
    }
  }

}

class ParseCwdoc{

  val cleanpipeline: CleanContentTxt = new CleanContentTxt(64)

  def bytes2Docstr(queryid: Int, doc: Array[Byte], cwrecord: String): (Int, String, String) = {
    val cwdoc: ClueWebWarcRecord = Class.forName(cwrecord).newInstance().asInstanceOf[ClueWebWarcRecord]
    cwdoc.readFields(new DataInputStream(new ByteArrayInputStream(doc)))
    println(queryid + " " + cwdoc.getDocid)
    val webpage: Document = Jsoup.parse(cwdoc.getContent)
    if (webpage.body != null) {
      (queryid, cwdoc.getDocid, cleanpipeline.cleanTxtStr(webpage, cwdoc.getDocid, true))
    } else {
      (queryid, cwdoc.getDocid, null)
    }

  }

}




trait DocVec{

  def readTermStat(sc : SparkContext) : Map[String, (Map[String,(Int, Int, Int)], Int, String)]={
    List("cw09", "cw12")
      .par
      .map {
        year => {
          year ->
            // Map(year, (Broadcast(Map(term,(idx, df, cf))), cardinality, cluewebrecord))
            ( {
              val termStat: Array[Array[String]] = sc
                .textFile("/user/khui/data/cwterms/term-df-cf-sort-" + year, 100)
                .map(line => line.split("\\s+"))
                .filter { termdfcf => termdfcf.length == 3 }
                .filter(cols => cols(1).toLong < Int.MaxValue)
                .filter(cols => cols(2).toLong < Int.MaxValue)

                //.filter(cols => cols(1).toLong > 5000)

                .collect()
              val bcval = readTermDfCf(termStat)
              println("Finished read in termStat for " + year + " with terms: " + termStat.length)
              bcval
            }, {
              year match {
                case "cw09" => GlobalConstants.CARDINALITY_CW09
                case "cw12" => GlobalConstants.CARDINALITY_CW12
              }
            }, {
              year match {
                case "cw09" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord"
                case "cw12" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord"
              }
            }
              )
        }
      }
      .seq
      .toMap


  }

  def broadcastTermStat(sc : SparkContext,partitionNum : Int=24, years: Array[String]=Array("cw09","cw12")) : Map[String, (Broadcast[Map[String,(Int, Int, Int)]], Int, String)]={
    years
      .map {
        year => {
          year ->
            // Map(year, (Broadcast(Map(term,(idx, df, cf))), cardinality, cluewebrecord))
            ( {

              val tokenizeTerm = (originTerm : String, tokenizer: Option[TokenizerFactory]) => {
                if (tokenizer.isEmpty){
                  Some(originTerm)
                } else {
                  val termlist = tokenizer.get.create(originTerm).getTokens
                  if (termlist.size() == 1){
                    Some(termlist.get(0))
                  } else {
                    None
                  }
                }
              }
              // term -> (idx, df, cf)
              val termIdxDfCf: Map[String, (Int, Int, Int)] = sc
                .textFile("/user/khui/data/cwterms/term-df-cf-idx-" + year, partitionNum)
                .map(line => line.split("\\s+"))
                .filter { termdfcf => termdfcf.length == 4 }
                .filter(cols => cols(1).toLong < Int.MaxValue && cols(2).toLong < Int.MaxValue)
                .map(cols => cols(0)-> (cols(3).toInt, cols(1).toInt, cols(2).toInt))
                .filter(cols => cols._1.nonEmpty)
                .map(cols=>(cols._1, cols._2))
                .collectAsMap()
                .toMap
              val bcval: Broadcast[Map[String, (Int, Int, Int)]] = sc.broadcast(termIdxDfCf)
              println("Finished read in termStat for " + year + " with terms: " + termIdxDfCf.size)
              bcval
            }, {
              year match {
                case "cw09" => GlobalConstants.CARDINALITY_CW09
                case "cw12" => GlobalConstants.CARDINALITY_CW12
              }
            }, {
              year match {
                case "cw09" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord"
                case "cw12" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord"
              }
            }
              )
        }
      }
      .toMap
  }


  def broadcastTermStatTokenizer(sc : SparkContext, tokenizer: TokenizerFactory, partitionNum : Int=24, years: Array[String]=Array("cw09","cw12")) : (Map[String, (Broadcast[Map[String,(Int, Int, Int)]], Int, String)], Set[String])={

    val termStat: Map[String, (Broadcast[Map[String, (Int, Int, Int)]], Int, String)] =
    years
      .map {
        year => {
          year ->
            // Map(year, (Broadcast(Map(term,(idx, df, cf))), cardinality, cluewebrecord))
            ( {

              // term -> (idx, df, cf)
              val termIdxDfCf: Map[String, (Int, Int, Int)] = sc
                .textFile("/user/khui/data/cwterms/term-df-cf-idx-" + year, partitionNum)
                .map(line => line.split("\\s+"))
                .filter { termdfcf => termdfcf.length == 4 }
                .filter(cols => cols(1).toLong < Int.MaxValue && cols(2).toLong < Int.MaxValue)
                .map(cols => cols(0)-> (cols(3).toInt, cols(1).toInt, cols(2).toInt))
                .collectAsMap()
                .map(t2 => tokenizer.create(t2._1).getTokens -> t2._2)
                .filter(t2 => t2._1.size() == 1)
                .map(t2 => t2._1.get(0) -> t2._2)
                .toMap

              val bcval: Broadcast[Map[String, (Int, Int, Int)]] = sc.broadcast(termIdxDfCf)
              println("Finished read in termStat for " + year + " with terms: " + termIdxDfCf.size)
              bcval
            },
              {
              year match {
                case "cw09" => GlobalConstants.CARDINALITY_CW09
                case "cw12" => GlobalConstants.CARDINALITY_CW12
              }
            }, {
              year match {
                case "cw09" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord"
                case "cw12" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord"
              }
            }
              )
        }
      }
      .toMap
/*
    val terms: Set[String] = termStat.flatMap(v => v._2._1.value.keySet).toSet
    println("In total, read in terms " + terms.size)*/
    (termStat, Set.empty[String])
  }

  def termIdxDfCf(sc : SparkContext,year:String, partitionNum : Int=24, tokenizer: Option[TokenizerFactory]=None): Map[String, (Int, Int, Int)] = {
    val tokenizerfunc: (String) =>  Array[String] = (term : String) => {
      if (tokenizer.isEmpty) {
        Array(term)
      } else {
       tokenizer.get.create(term).getTokens.toArray[String](Array.empty[String])
      }
    }
    sc
      .textFile("/user/khui/data/cwterms/term-df-cf-idx-" + year, partitionNum)
      .map(line => line.split("\\s+"))
      .filter { termdfcf => termdfcf.length == 4 }
      .filter(cols => cols(1).toLong < Int.MaxValue && cols(2).toLong < Int.MaxValue)
      .map(cols => cols(0) ->(cols(3).toInt, cols(1).toInt, cols(2).toInt))
      .collectAsMap()
      .map(t2 => tokenizerfunc(t2._1) -> t2._2)
      .filter(t2 => t2._1.length == 1)
      .map(t2 => t2._1(0) -> t2._2)
      .toMap
  }



  def termStatTokenizer(sc : SparkContext, tokenizer: TokenizerFactory, partitionNum : Int=24, years: Array[String]=Array("cw09","cw12")) : Map[String, (Map[String,(Int, Int, Int)], Int, String)]={

    val termStat: Map[String, (Map[String, (Int, Int, Int)], Int, String)] =
      years
        .map {
          year => {
            year ->
              // Map(year, (Broadcast(Map(term,(idx, df, cf))), cardinality, cluewebrecord))
              ( {

                // term -> (idx, df, cf)
                val termIdxDfCf: Map[String, (Int, Int, Int)] = sc
                  .textFile("/user/khui/data/cwterms/term-df-cf-idx-" + year, partitionNum)
                  .map(line => line.split("\\s+"))
                  .filter { termdfcf => termdfcf.length == 4 }
                  .filter(cols => cols(1).toLong < Int.MaxValue && cols(2).toLong < Int.MaxValue)
                  .map(cols => cols(0)-> (cols(3).toInt, cols(1).toInt, cols(2).toInt))
                  .collectAsMap()
                  .map(t2 => tokenizer.create(t2._1).getTokens -> t2._2)
                  .filter(t2 => t2._1.size() == 1)
                  .map(t2 => t2._1.get(0) -> t2._2)
                  .toMap

                println("Finished read in termStat for " + year + " with terms: " + termIdxDfCf.size)
                termIdxDfCf
              },
                {
                  year match {
                    case "cw09" => GlobalConstants.CARDINALITY_CW09
                    case "cw12" => GlobalConstants.CARDINALITY_CW12
                  }
                }, {
                year match {
                  case "cw09" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb09WarcRecord"
                  case "cw12" => "de.mpii.docsimilarity.mr.input.clueweb.ClueWeb12WarcRecord"
                }
              }
                )
          }
        }
        .toMap
    termStat
  }



  def mkstring(cwid: String, termtfidf: Map[Int, Double]): String = {
    termtfidf
      .toArray
      .map(t2 => t2._1->Math.round(t2._2 * 1E8) * 1E-8 )
      .filter(t2 => t2._2 != 0)
      .sortBy(t2 => t2._1)
      .map(t2 => t2._1 + ":" + "%.4E".format(t2._2))
      .+:(cwid)
      .mkString(" ")
  }


  def readTermDfCf(lines: Array[Array[String]], tokenizer: TokenizerFactory=null): Map[String, (Int, Int, Int)] = {
    if (tokenizer == null) {
      val termIdxDfCf: Map[String, (Int, Int, Int)] =
        Array
          .tabulate(lines.length) { idx => lines(idx)(0) ->(idx, lines(idx)(1).toInt, lines(idx)(2).toInt) }
          .toMap
      termIdxDfCf
    } else {
      val termIdxDfCf: Map[String, (Int, Int, Int)] = lines
        .map(cols => tokenizer.create(cols(0)).getTokens-> cols)
        .filter(t2 => t2._1.size() == 1)
        .zipWithIndex
        .map(t2 => t2._1._1.get(0) -> (t2._2, t2._1._2(1).toInt, t2._1._2(2).toInt))
        .toMap
      termIdxDfCf
    }
  }

  def recursiveListFiles(f: String): Array[String] = {
    val fs = FileSystem.get(new Configuration())
    val status_list: Array[String] = fs.listStatus(new Path(f))
      .map(s => s.getPath)
      .filter(p => !p.getName.startsWith("_"))
      .map(_.toString)
    status_list
  }

}
