package com.mariosg92


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File
import scala.util.Try

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Goodreads Books Dataset")
      .getOrCreate()


    val ruta = args(0)
    val bookFiles = fileSystemExe(args(2), ruta)


    val dfList = bookFiles.map { f =>
      spark.read.option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("multiLine", "true") // Hay saltos de líneas en algunos registros
        .csv(f)
    }

    // A continuación voy a ordenar todos los dataframes, y utilizo la función creada anteriormente para rellenar con null si no existe esa columna
    val dfList2 = dfList.map { df =>
      df.withColumn("Description", hasColumn(df, "Description"))
        .withColumn("Count of text reviews", hasColumn(df, "Count of text reviews"))
    }
    // Una vez ordenado realizo la union de los dataframes, utilizo el distinct ya que tenemos unos cuantos datos duplicados
    val unionDF = dfList2.reduce(_ unionByName _).distinct

    // Doy formato y limpio datos de las columnas RatingDist5,4,3,2,1
    // También me doy cuenta que el PublishMonth y el PublishDay están cambiados ya que los valores de PublishDay van de 1 a 12

    val df = unionDF.withColumn("Id", col("Id").cast(IntegerType))
      .withColumn("Rating", col("Rating").cast(DoubleType))
      .withColumn("PublishYear", col("PublishYear").cast(IntegerType))
      .withColumn("PublishMonth", col("PublishMonth").cast(IntegerType))
      .withColumn("PublishDay", col("PublishDay").cast(IntegerType))
      .withColumn("RatingDist5", regexp_extract(col("RatingDist5"), """5:(\d+)""", 1).cast(IntegerType))
      .withColumn("RatingDist4", regexp_extract(col("RatingDist4"), """4:(\d+)""", 1).cast(IntegerType))
      .withColumn("RatingDist3", regexp_extract(col("RatingDist3"), """3:(\d+)""", 1).cast(IntegerType))
      .withColumn("RatingDist2", regexp_extract(col("RatingDist2"), """2:(\d+)""", 1).cast(IntegerType))
      .withColumn("RatingDist1", regexp_extract(col("RatingDist1"), """1:(\d+)""", 1).cast(IntegerType))
      .withColumn("RatingDistTotal", regexp_extract(col("RatingDistTotal"), """total:(\d+)""", 1).cast(IntegerType))
      .withColumn("CountsOfReview", col("CountsOfReview").cast(IntegerType))
      .withColumn("pagesNumber", col("pagesNumber").cast(IntegerType))
      .withColumn("Count of text reviews", col("Count of text reviews").cast(IntegerType))
      .withColumnRenamed("Count of text reviews", "CountOfTextReviews")
      .withColumnRenamed("PublishMonth", "PublishDay1")
      .withColumnRenamed("PublishDay", "PublishMonth")
      .withColumnRenamed("PublishDay1", "PublishDay")

    // Cambio nulls y redondeo el Rating a 2 decimales
    val cleanDF = df.withColumn("Language", when(col("Language").isNotNull, col("Language")).otherwise("n/a"))
      .withColumn("ISBN", when(col("ISBN").isNotNull, col("ISBN")).otherwise("n/a"))
      .withColumn("Description", when(col("Description").isNotNull, col("Description")).otherwise("Description not available"))
      .withColumn("CountOfTextReviews", when(col("CountOfTextReviews").isNotNull, col("CountOfTextReviews")).otherwise(0))
      .withColumn("Rating", round(col("Rating"), 2))

    cleanDF.write.format("parquet").save(args(1))
  }

  // Función para comprobar que existe la columna, ya que hay files con menos columnas
  def hasColumn(df: DataFrame, path: String): Column = {
    if (Try(df(path)).isSuccess) {
      df(path)
    } else {
      lit(null)
    }
  }

  // Recojo el nombre de los archivos .csv de los libros
  def getListOfFiles(dir: String): Array[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile)
        .filter(_.getName.contains("book"))
        .map(_.getPath).toArray
    } else {
      Array[String]()
    }
  }
  // Función para determinar si estoy utilizando hdfs o el sistema de archivos local a la hora de coger los datasets
  def fileSystemExe(fs: String, ruta: String): Array[String] = {
    if (fs.equals("hdfs")) {
      val fs = FileSystem.get(new Configuration())
      val status = fs.listStatus(new Path(ruta))
      val files = status.map(x => x.getPath.toString)
      val bookFiles = files.filter(_.contains("book"))
      bookFiles
    } else {
      val bookFiles = getListOfFiles(ruta)
      bookFiles
    }
  }

}
