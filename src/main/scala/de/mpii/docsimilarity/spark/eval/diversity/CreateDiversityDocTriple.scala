package de.mpii.docsimilarity.spark.eval.diversity

import de.mpii.docsimilarity.spark.doc2vec.VectorizeDoc
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by khui on 01/11/15.
 */
object CreateDiversityDocTriple {

  private var inputdir: String = ""
  private var outputdir: String = ""
  private var parNumber: Int = 1024

  def parse(args: List[String]): Unit = args match {
    case ("-i") :: dir :: tail =>
      inputdir = dir
      parse(tail)
    case ("-o") :: file :: tail =>
      outputdir = file
      parse(tail)
    case ("-c") :: coreNum :: tail =>
      parNumber = coreNum.toInt
      parse(tail)
    case (Nil) =>
  }


  def main(args: Array[String]): Unit = {
    parse(args.toList)
    val conf: SparkConf = new SparkConf()
      .setAppName("Create doc triple for diversity triple banchmark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/GW/D5data-2/khui/cw-docvector-termdf/tmp")
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
    val fs: FileSystem = FileSystem
      .get(sc.hadoopConfiguration)
    if (fs.exists(new Path(outputdir))) {
      println(outputdir + " is being removed")
      fs.delete(new Path(outputdir), true)
    }

    fs.listStatus(new Path(inputdir))
      .filter(f => f.getPath.getName.startsWith("qrels.diversity.wt"))
      .map(f => f.getPath)
      .par
      .foreach(
        p => {
          val year = p.getName.split("\\.")(2)
          val qidrange: Range =
            year match {
              case "wt09" => 1 to 50
              case "wt10" => 51 to 100
              case "wt11" => 101 to 150
              case "wt12" => 151 to 200
              case "wt13" => 201 to 250
              case "wt14" => 251 to 300
            }
          qidrange
            .par
            .filter(qid => qid > 100)
            .foreach(qid => Qrel2SubtopicTriple(sc, p.toString, outputdir, qid))
          println(p + " finished.")
        }

      )

  }

  /**
    * create document triple, so that,
    * (d1, d2, dn)
    * subtopic(d1) \intersect subtopic(d2) is not empty
    * meanwhile
    * (subtopic(d1) union subtopic(d2)) \intersect subtopic(dn) is empty
    *
    * @param sc
    * @param qrelf
    * @param outdir
    * @param qid
    */
  def Qrel2SubtopicTriple(sc: SparkContext, qrelf: String, outdir: String, qid: Int): Unit = {
    // read in document that has been labeled relevant at least to one subtopic/facet
    val cwidSubtopicIdx: RDD[((String, Set[Int]), Long)] = sc.textFile(qrelf, parNumber)
      .map(line => line.split(" "))
      // only retain documents that are at least relevant to one subtopic
      .filter(cols => cols(0).toInt == qid && cols(3).toInt > 0)
      // cwid -> subtopic
      .map(cols => cols(2) -> cols(1).toInt)
      // cwid -> Set(subtopic)
      .aggregateByKey[Set[Int]](Set.empty[Int])((a, s)=> a.+(s), (a1, a2) => a1.++(a2))
      // cwid -> Set(subtopic), idx
      .zipWithIndex()
      .cache()

    // generate (d1, d2) that share at least 1 subtopic
    val docPairSubtopics: RDD[((String, String), (Set[Int], Long, Long))] =
    cwidSubtopicIdx
      .cartesian(cwidSubtopicIdx)
      // upper triangle
      .filter{case(d1, d2) => d1._2 > d2._2}
      // for first two documents, select documents that at least share one subtopic
      .filter{case(d1, d2) => d1._1._2.intersect(d2._1._2).nonEmpty}
      // (cwid1, cwid2) -> (union of subtopics, idx1, idx2)
      .map{case(d1, d2) => (d1._1._1, d2._1._1) -> (d1._1._2.union(d2._1._2), d1._2, d2._2)}

    // generate the document triple
    val triples: RDD[String] =
    docPairSubtopics
      .cartesian(cwidSubtopicIdx)
      // filter out the document that already in the pair
      .filter{case(docpair, doc) => doc._2 != docpair._2._2 && doc._2 != docpair._2._3}
      // for the third document, its subtopic set should be disjointed with the first two documents
      .filter{case(docpair, doc) => doc._1._2.intersect(docpair._2._1).isEmpty}
      // map to the output format: cwid1, cwid2, cwidn
      .map{case(docpair, doc) => Array(docpair._1._1, docpair._1._2, doc._1._1).mkString(" ")}
      .repartition(1)
      .cache()

    val numberOfTriple = triples.count()
    if (numberOfTriple > 0) {
      triples
        .saveAsTextFile(outputdir + "/" + qid)
    }

    triples.unpersist()
    cwidSubtopicIdx.unpersist()
    println("Finished for query " + qid + " with triples " + numberOfTriple)
  }


  def Qrel2RelrelIrrelTriple(sc: SparkContext, qrelf: String, outdir: String, qid: Int): Unit = {
    val qrellines: RDD[(Int, (String, Int))] = sc.textFile(qrelf, parNumber)
      .map(line => line.split(" "))
      .filter(cols => cols(0).toInt == qid && cols(3).toInt > 0)
      // subtopic -> (cwid, label)
      .map(cols => cols(1).toInt ->(cols(2), cols(3).toInt))
      .cache()



    val subtopics: Array[Int] = qrellines.map(v => v._1).distinct.collect()


    if (subtopics.length == 1) {
      println("Query ERROR " + qid + " " + qrellines.count() + " " + subtopics.length)
      return
    }

    println("Query " + qid + " " + qrellines.count() + " " + subtopics.length)

    // store all result triples
    val buffers =
      subtopics
        .toSeq
        .par
        .map(
          subtopic => {
            val rels: RDD[((Int, (String, Int)), Long)] =
              qrellines
                .filter(v => v._1 == subtopic)
                .zipWithIndex()
                .cache()

            val relCwids: Set[String] = rels.map(v => v._1._2._1).collect().toSet

            val relpairs: RDD[(Int, (String, Int), (String, Int))] =
              rels
                .cartesian(rels)
                .filter(pair => pair._1._2 > pair._2._2 && pair._1._1._2._1 != pair._2._1._2._1)
                .map(p2 => (subtopic, p2._1._1._2, p2._2._1._2))

            val nrels: RDD[(Int, (String, Int))] =
              qrellines
                .filter(v => v._1 != subtopic && (!relCwids.contains(v._2._1)))
                .cache()


            val triples: RDD[String] =
              relpairs
                .cartesian(nrels)
                .map { case (pr, n) => Array(pr._2._1, pr._3._1, n._2._1, pr._1, n._1, pr._2._2, pr._3._2, n._2._2).mkString(" ") }
                .cache()

            println("Query " + qid + " subtopic " + subtopic + " rel-nrel-triple " + rels.count() + " " + nrels.count() + " " + triples.count())
            rels.unpersist()
            nrels.unpersist()

            triples
          }
        ).seq

    val results: RDD[String] =
    sc
      .union(buffers)
      .coalesce(1)
      .cache()

    results
      .saveAsTextFile(outputdir + "/" + qid)

    println("Finished for query " + qid + " with " + subtopics.length + " subtopics " + " and " + results.count() + " triples.")

    qrellines.unpersist()
    results.unpersist()
  }

}
