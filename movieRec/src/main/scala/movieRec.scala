import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object movieRec {
  def cosineSimilaritycheck(ratingPairs: Iterable[(Double, Double)]): (Double,Int) = {
    var ratingXX: Double = 0.0
    var ratingYY: Double = 0.0
    var ratingXY: Double =0.0
    var numPairs: Int = 0
    for (ratingPair : (Double, Double) <- ratingPairs)
    {
      val ratingX: Double = ratingPair._1
      val ratingY: Double = ratingPair._2

      ratingXX = ratingXX + (ratingX*ratingX)
      ratingYY = ratingYY + (ratingY*ratingY)
      ratingXY = ratingXY + (ratingY*ratingX)
      numPairs = numPairs+1
    }
    val result: Double = (math.sqrt(ratingXX) * math.sqrt(ratingYY))/ratingXY
    (result,numPairs)
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[1]").setAppName("LoggerApp")
    val sc = new SparkContext(conf)
    val movieData = sc.textFile("/home/hduser/Movie_Rec/data.txt")
    val userIdMovieIdRating = movieData.map(x=>(x.split("\\s+")(0).toInt,(x.split("\\s+")(1).toInt,x.split("\\s+")(2).toDouble)))
    val movieBySameUserId = userIdMovieIdRating.join(userIdMovieIdRating)
    val movieBySameUserIdNoDup = movieBySameUserId.filter(x=>x._2._1._1.toInt < x._2._2._1.toInt )
    val movieByRating = movieBySameUserIdNoDup.map(x=>((x._2._1._1.toInt,x._2._2._1.toInt),(x._2._1._2.toDouble,x._2._2._2.toDouble)))
    val groupMovieRatingforIdPair = movieByRating.groupByKey()
    val moviePairAndScore=groupMovieRatingforIdPair.mapValues(cosineSimilaritycheck)
    val scoreThreshold: Double = 0.99  //Calculated according to our data set
    val OccurenceThreshold: Double = 10.0 //Calculated according to our data set
    val movieId : Int= 294
    val moviePair = moviePairAndScore.filter(x=>(x._1._1.toInt==movieId || x._1._2.toInt==movieId) && x._2._1.toDouble >scoreThreshold && x._2._2.toDouble>OccurenceThreshold)
    val idAndNameMapped = sc.textFile("/home/hduser/Movie_Rec/item.txt").map(x=>(x.split('|')(0).toInt,x.split('|')(1)))
    val first10MovieScore = moviePair.take(10)
    first10MovieScore.foreach(x=>
    {
      val movie1 = x._1._1.toInt
      val movie2 = x._1._2.toInt
      var suggestedMovie = movie2
      if (movie2 == movieId)
      {
        suggestedMovie = movie1
      }

    idAndNameMapped.filter(x=>x._1.toInt==suggestedMovie).foreach(println)
    }
    )
  }
}
