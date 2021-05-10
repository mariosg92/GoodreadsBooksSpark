// Databricks notebook source
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

// Función para comprobar que existe la columna, ya que hay files con menos columnas
def hasColumn(df: DataFrame, path: String) = {
  if (Try(df(path)).isSuccess){
    df(path)
  }else{
    lit(null)
  }
}
// Recojo las rutas de todos los archivos dbutils es una herramienta de databricks
val bookFiles = dbutils.fs.ls("dbfs:/FileStore/tables/datasets").map(_.path).filter(_.contains("book"))   
val dfList = bookFiles.map{ file => spark.read.option("header","true")
                           .option("quote", "\"")
                           .option("escape", "\"")
                           .option("multiLine","true")  // Hay saltos de líneas en algunos registros
                           .csv(file)}  
// A continuación voy a ordenar todos los dataframes, y utilizo la función creada anteriormente para rellenar con null si no existe esa columna
val dfList2 = dfList.map{df =>  df.withColumn("Description", hasColumn(df, "Description"))
                                  .withColumn("Count of text reviews",hasColumn(df,"Count of text reviews"))
                                  .select("Id", "Name", "Authors", "ISBN", "Rating", "PublishYear", "PublishMonth", "PublishDay", "Publisher", "RatingDist5", "RatingDist4", "RatingDist3", "RatingDist2", "RatingDist1", "RatingDistTotal", "CountsOfReview", "Language", "pagesNumber","Description","Count of text reviews")}
// Una vez ordenado realizo la union de los dataframes, utilizo el distinct ya que tenemos unos cuantos datos duplicados
val unionDF = dfList2.reduce(_ union _).distinct 



// COMMAND ----------

// Doy formato y limpio datos de las columnas RatingDist5,4,3,2,1
// También me doy cuenta que el PublishMonth y el PublishDay están cambiados ya que los valores de PublishDay van de 1 a 12

val df = unionDF.withColumn("Id",$"Id".cast(IntegerType))
  .withColumn("Rating",$"Rating".cast(DoubleType))
  .withColumn("PublishYear",$"PublishYear".cast(IntegerType))
  .withColumn("PublishMonth",$"PublishMonth".cast(IntegerType))
  .withColumn("PublishDay",$"PublishDay".cast(IntegerType))
  .withColumn("RatingDist5",regexp_extract($"RatingDist5","""5:(\d+)""",1).cast(IntegerType))
  .withColumn("RatingDist4",regexp_extract($"RatingDist4","""4:(\d+)""",1).cast(IntegerType))
  .withColumn("RatingDist3",regexp_extract($"RatingDist3","""3:(\d+)""",1).cast(IntegerType))
  .withColumn("RatingDist2",regexp_extract($"RatingDist2","""2:(\d+)""",1).cast(IntegerType))
  .withColumn("RatingDist1",regexp_extract($"RatingDist1","""1:(\d+)""",1).cast(IntegerType))
  .withColumn("RatingDistTotal",regexp_extract($"RatingDistTotal","""total:(\d+)""",1).cast(IntegerType))
  .withColumn("CountsOfReview",$"CountsOfReview".cast(IntegerType))
  .withColumn("pagesNumber",$"pagesNumber".cast(IntegerType))
  .withColumn("Count of text reviews",$"Count of text reviews".cast(IntegerType))
  .withColumnRenamed("Count of text reviews","CountOfTextReviews")
  .withColumnRenamed("PublishMonth","PublishDay1")
  .withColumnRenamed("PublishDay","PublishMonth")
  .withColumnRenamed("PublishDay1","PublishDay")

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.where($"Id".isNull).count

// COMMAND ----------

// Cambio nulls y redondeo el Rating a 2 decimales
val cleanDF2 = cleanDF.withColumn("Language",when($"Language".isNotNull, $"Language").otherwise("n/a"))
  .withColumn("ISBN",when($"ISBN".isNotNull, $"ISBN").otherwise("n/a"))
  .withColumn("Description",when($"Description".isNotNull, $"Description").otherwise("Description not available"))
  .withColumn("CountOfTextReviews",when($"CountOfTextReviews".isNotNull, $"CountOfTextReviews").otherwise(0))
  .withColumn("Rating",round($"Rating",2))

// COMMAND ----------

display(cleanDF2)

// COMMAND ----------

// Leo los otros archivos realacionados con los ratings dados por usuarios
val userRDF = spark.read.format("csv")
  .option("header","true")
  .option("quote", "\"")
  .option("escape", "\"")
  .load("/FileStore/tables/datasets/user*.csv")
userRDF.printSchema
userRDF.cache()

// COMMAND ----------

// Me he creado un dataframe pivoteando los valores de los ratings por usuarios y limpiado los nulls, se podría utilizar para ver si tiene un buen rating o no un libro
val pivotDF = userRDF.groupBy("Name")
  .pivot("Rating")
  .count()
val booksRatingDF = pivotDF.select("Name","it was amazing","really liked it","liked it","it was ok","did not like it")
  .withColumn("it was amazing",when($"it was amazing".isNull, 0).otherwise($"it was amazing"))
  .withColumn("really liked it",when($"really liked it".isNull, 0).otherwise($"really liked it"))
  .withColumn("liked it",when($"liked it".isNull, 0).otherwise($"liked it"))
  .withColumn("it was ok",when($"it was ok".isNull, 0).otherwise($"it was ok"))
  .withColumn("did not like it",when($"did not like it".isNull, 0).otherwise($"did not like it"))
booksRatingDF.show(10, false)

// COMMAND ----------

// Lo guardo en parquet para realizar las consultas
cleanDF2.write.format("parquet").save("/FileStore/tables/datasets/parquet")

// COMMAND ----------

val columnarBooksDF = spark.read.format("parquet").load("/FileStore/tables/datasets/parquet/*.parquet")
columnarBooksDF.cache()
columnarBooksDF.printSchema

// COMMAND ----------

// MAGIC %md 1.- Rating promedio de todos los libros

// COMMAND ----------

columnarBooksDF.select(round(avg("Rating"),2).as("Average Rating")).show()

// COMMAND ----------

// MAGIC %md 2.- Rating promedio de los libros por autor

// COMMAND ----------

columnarBooksDF.groupBy("Authors")
  .agg(round(avg("Rating"),2).as("Average Rating"))
  .show()

// COMMAND ----------

// MAGIC %md 3.- Rating promedio de los libros por Publisher

// COMMAND ----------

columnarBooksDF.groupBy("Publisher")
  .agg(round(avg("Rating"),2).as("Average Rating"))
  .show()

// COMMAND ----------

// MAGIC %md 4.- Número promedio de páginas de todos los libros

// COMMAND ----------

columnarBooksDF.select(round(avg("pagesNumber"),2).as("Average Pages")).show()

// COMMAND ----------

// MAGIC %md 5.- Número promedio de páginas de todos los libros por autor

// COMMAND ----------

columnarBooksDF.groupBy("Authors")
  .agg(round(avg("pagesNumber"),2).as("Average Pages"))
  .show()

// COMMAND ----------

// MAGIC %md 6.- Número promedio de páginas de todos los libros por Publisher

// COMMAND ----------

columnarBooksDF.groupBy("Publisher")
  .agg(round(avg("pagesNumber"),2).as("Average Pages"))
  .show()

// COMMAND ----------

// MAGIC %md 7.- Número promedio de libros publicados por autor

// COMMAND ----------

columnarBooksDF.groupBy("Authors")
  .count()
  .agg(round(avg("count"),2).as("Average Books"))
  .show()

// COMMAND ----------

// MAGIC %md 8.- Ordenar los libros de mayor a menor (Top 15) por número de ratings dados por usuarios (excluir aquellos valores sin rating)

// COMMAND ----------

userRDF.groupBy("Name")
  .count()
  .where($"Name" =!= "Rating")
  .select($"Name",$"count".as("Ratings Received"))
  .orderBy(desc("count"))
  .show(false)

// COMMAND ----------

// MAGIC %md 9.- Obtener Top 5 de ratings más frecuentes otorgados por usuarios

// COMMAND ----------

userRDF.groupBy("Rating")
  .count()
  .orderBy(desc("count"))
  .limit(5)
  .show()

// COMMAND ----------

cleanDF2
  .where($"Id" === 1)
  .show()
