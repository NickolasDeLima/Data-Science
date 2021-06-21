from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# Esta função apenas cria um "dicionário" Python que mais tarde podemos
# usar para converter IDs de filmes em nomes de filmes durante a impressão
# dos resultados finais.
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # cria uma sessão spark
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # carrega o u.item no dicionário 
    movieNames = loadMovieNames()

    # carrega o u.data 
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    
    # converte para uma RDD de Row objects com (movieID, rating)
    movies = lines.map(parseInput)
    
    # Converte para um DataFrame
    movieDataset = spark.createDataFrame(movies)

    # computa o rating médio para cada movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # computa a quantidade de ratings que cada movieID teve
    counts = movieDataset.groupBy("movieID").count()

    # junta os 2 conjunto de dados (movieID, avg(rating), count)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # filtra os filmes com 10 ou mais ratings
    popularAveragesAndCounts = averagesAndCounts.filter("count > 10")

    # seleciona os 10 melhores resultado
    topTen = popularAveragesAndCounts.orderBy("avg(rating)").take(10)

    # mostra o resultado 
    # converte movieID em nomes.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    spark.stop()