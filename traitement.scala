import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.rdd.RDD
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Tokenizer

import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Tokenizer
// import com.johnsnowlabs.nlp.annotators.SentimentDLModel

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

import scala.collection.JavaConverters._

import java.util.Properties

object Main extends App {
  val spark = SparkSession.builder
    .appName("SplitBooks")
    .master("local")
    .config("fs.permissions.umask-mode", "000")
    .getOrCreate()

  // val wordsList = BookProcessor.processBooks("books_large_p1.txt", spark)
  val filePath = "output/book_1.txt"
  val dictionnary = "sentiment_dictionnary.txt"
  // SentimentAnalyzer.analyzeSentimentOccurence(filePath, spark)
  SentimentAnalyzer.analyzeSentimentWeigth(filePath, spark)

  spark.stop()
}

object BookProcessor {
  def processBooks(filePath: String, spark: SparkSession): Unit = {
    // Définition du schéma du DataFrame pour lire les fichiers books_large
    val customSchema = StructType(
      Array(
        StructField("line", StringType, true)
      )
    )

    val linesDF = spark.read
      .option("header", "false")
      .schema(customSchema)
      .text(filePath)
    // .limit(4000)

    // Ajouter une colonne avec l'indice de la ligne
    val linesWithIndexDF =
      linesDF.withColumn("lineIndex", monotonically_increasing_id())

    // Compter le nombre de lignes contenant "isbn"
    val countIsbn =
      linesWithIndexDF.filter(col("line").contains("isbn")).count()

    // Trouver les indices des lignes contenant "isbn"
    val isbnIndices = linesWithIndexDF
      .filter(col("line").contains("isbn"))
      .select("lineIndex")
      .collect()
      .map(_.getLong(0))

    // Créer des paires d'indices pour délimiter les livres
    val bookIndices = (0L +: isbnIndices).zip(isbnIndices :+ Long.MaxValue)

    // Séparer les livres en DataFrames distincts
    val bookDataFrames = bookIndices.map { case (startIndex, endIndex) =>
      linesWithIndexDF
        .filter(col("lineIndex").between(startIndex, endIndex))
        .select("line")
    }

    linesWithIndexDF.select("lineIndex").show()

    println(s"Nombre de livres : ${bookDataFrames.size}")

    // Afficher les premières lignes des 3 premiers livres
    // bookDataFrames.take(3).zipWithIndex.foreach { case (bookDF, index) =>
    //   val firstLine = bookDF.first().getString(0)
    //   println(s"Le livre $index, première ligne : $firstLine")
    // }

    bookDataFrames.zipWithIndex.foreach { case (bookDF, index) =>
      val fileName = s"output/book_$index.txt"
      bookDF.rdd.map(_.getString(0)).toLocalIterator.grouped(1000).foreach {
        lines =>
          val writer = new BufferedWriter(new FileWriter(fileName, true))
          lines.foreach { line =>
            writer.write(line + "\n")
          }
          writer.close()
      }
      println(s"Le livre $index a été écrit dans $fileName")
    }
  }
}

object SentimentAnalyzer {
  def analyzeSentimentOccurence(filePath: String, spark: SparkSession): Unit = {
    val textRDD = spark.sparkContext.textFile(filePath)

    val wordsRDD: RDD[String] = textRDD
      .flatMap(line => line.split("\\s+"))
      .map(word => word.replaceAll("[^\\p{L}\\p{Nd}]+", "").toLowerCase)
      .filter(_.nonEmpty)
      .filter(_.length > 2)

    // Liste des stopwords
    val remover = new StopWordsRemover()
    val stopWords = remover.getStopWords

    val filteredWordsRDD = wordsRDD.filter(word => !stopWords.contains(word))

    // Occurrence de chaque mot
    val wordCounts: RDD[(String, Int)] = filteredWordsRDD
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // Occurrence dans l'ordre décroissant
    val sortedWordCounts = wordCounts.sortBy(-_._2)

    val writer = new PrintWriter("sentiment_dictionary.txt")
    try {
      sortedWordCounts.collect().foreach { case (word, count) =>
        writer.println(s"$word : $count")
      }
    } finally {
      writer.close()
    }
  }

}
