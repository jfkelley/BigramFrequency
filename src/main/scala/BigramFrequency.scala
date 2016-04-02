import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.annotation.tailrec
import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter

object BigramFrequency {
  
  def main(args: Array[String]): Unit = {
    val settings = parseArgs(args)
    if (settings.getOrElse("spark", "false").toBoolean) {
      runSpark(settings)
    } else {
      runLocal(settings)
    }
  }
  
  def runSpark(settings: Map[String, String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("BigramFrequency")
    val sc = new SparkContext(conf)
    
    try {
      val docs = sc.wholeTextFiles(settings("input"), 10).map(_._2)
      val adjacentOnly = settings.getOrElse("adjacent", "false").toBoolean
      val bigrams = docs.flatMap(doc => extractBigrams(tokenize(doc), adjacentOnly))
      val counts =
        bigrams
        .map(pair => (pair, 1)) // start with (bigram, 1)
        .reduceByKey(_ + _) // group by bigram, add counts together
      counts.saveAsTextFile(settings("output"))
    } finally {
      sc.stop()
    }
  }
  
  def runLocal(settings: Map[String, String]): Unit = {
    val docFiles = new File(settings("input")).listFiles()
    val docs = docFiles.map { file => Source.fromFile(file).mkString }
    val adjacentOnly = settings.getOrElse("adjacent", "false").toBoolean
    val bigrams = docs.flatMap(doc => extractBigrams(tokenize(doc), adjacentOnly))
    val counts =
      bigrams
      .map(pair => (pair, 1)) // start with (bigram, 1)
      .groupBy(_._1) // group by bigram
      .mapValues(_.map(_._2).sum) // sum of counts
    
    val out = new PrintWriter(new FileWriter(settings("output")))
    try {
      counts.foreach(out.println)
    } finally {
      out.close()
    }
  }
  
  def tokenize(doc: String): IndexedSeq[String] = {
    doc.split("\\s+").filter(word => word.forall(_.isLetterOrDigit))
  }
  
  def extractBigrams(words: IndexedSeq[String], adjacentOnly: Boolean): Seq[(String, String)] = {
    if (adjacentOnly) {
      words.drop(1).zip(words.dropRight(1))
    } else {
      for (
        i <- 0 until words.size - 1;
        j <- i + 1 until words.size;
        if words(i) != words(j)
      ) yield (words(i), words(j))
    }
  }
  
  def parseArgs(args: Array[String]): Map[String, String] = {
    @tailrec def _parse(remaining: List[String], acc: Map[String, String]): Map[String, String] = remaining match {
      case Nil => acc
      case "--spark" :: rest => _parse(rest, acc + ("spark" -> "true"))
      case "--adjacent" :: rest => _parse(rest, acc + ("adjacent" -> "true"))
      case "--input" :: path :: rest => _parse(rest, acc + ("input" -> path))
      case "--output" :: path :: rest => _parse(rest, acc + ("output" -> path))
      case otherArg :: rest => throw new IllegalArgumentException("Unrecognized argument: " + otherArg)
    }
    _parse(args.toList, Map.empty)
  }
  
  
}