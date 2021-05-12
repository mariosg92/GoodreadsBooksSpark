# GoodreadsBooksSpark


- Aplicación Spark para la lectura y enriquecimiento de los datos del dataset: https://www.kaggle.com/bahramjannesarr/goodreads-book-datasets-10m, utilizado para hacer pruebas con clúster hadoop, es solamente para los archivos de books, ya que tenían una estructura irregular (diferente orden y número de columnas). <br/>
Para la correcta ejecución de la app se le debe pasar los siguientes argumentos, **input output** y un tercer argumento que dependerá donde corramos la aplicación. Si estamos realizando la ejecución sobre un clúster hadoop en hdfs, deberemos indicar **hdfs** como tercer argumento sino pondremos **local**.

- Añadido además el Notebook Scala trabajado en databricks con las siguientes consultas realizadas:
   - Rating promedio de todos los libros
   - Rating promedio de los libros por autor
   - Rating promedio de los libros por Publisher
   - Número promedio de páginas de todos los libros
   - Número promedio de páginas de todos los libros por autor
   - Número promedio de páginas de todos los libros por Publisher
   - Número promedio de libros publicados por autor
   - Ordenar los libros de mayor a menor (Top 15) por número de ratings dados por usuarios (excluir aquellos valores sin rating)
   - Obtener Top 5 de ratings más frecuentes otorgados por usuarios

- Notebook databricks: https://bit.ly/2RGH6D1


