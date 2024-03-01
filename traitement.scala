import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main extends App {
  val spark = SparkSession.builder.appName("SplitBooks").master("local").getOrCreate()

  val filePath = "books_large_p1.txt"

  // Définition du schéma du DataFrame
  val customSchema = StructType(Array(
    StructField("line", StringType, true)
  ))

  // Charger le fichier texte en un DataFrame avec une colonne "line"
  val linesDF = spark.read
    .option("header", "false")
    .schema(customSchema)
    .text(filePath)
    .limit(4000)

  // Ajouter une colonne avec l'indice de la ligne
  val linesWithIndexDF = linesDF.withColumn("lineIndex", monotonically_increasing_id())

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

  // Afficher la première ligne de chaque livre
    bookDataFrames.zipWithIndex.foreach { case (bookDF, index) =>
    val firstLine = bookDF.first().getString(0)
    println(s"Le livre $index, première ligne : $firstLine")
    }
  // Écrire chaque livre dans un fichier séparé
  bookDataFrames.zipWithIndex.foreach { case (bookDF, index) =>
//     val fileName = s"book_$index.txt"
//     bookDF.write.text(fileName)
//     println(s"Le livre $index a été écrit dans $fileName")
//   }

  // Arrêter la session Spark
  spark.stop()
}

