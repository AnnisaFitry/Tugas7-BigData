//Note: it works only in spark2
//To launch it on local mode, run the following command on cloudxlab console: spark-shell
//To launch it in yarn mode, run the following command on cloudxlab console: spark-shell --master yarn

import org.apache.spark.ml.recommendation.ALS
 
case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  assert(fields.size == 4)
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
}

//Test it
parseRating("1::1193::5::978300760")

var raw = sc.textFile("/data/ml-1m/ratings.dat")
//check one record. it should be res4: Array[String] = Array(1::1193::5::978300760)
//If this fails the location of file is wrong.
raw.take(1)

val ratings = raw.map(parseRating).toDF()
//check if everything is ok
ratings.show(5)

val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
//Alternating Least Squares (ALS) matrix factorization.
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

val model = als.fit(training)
model.save("mymodel")

//Prepare the recommendations
val predictions = model.transform(test)
predictions.map(r => r(2).asInstanceOf[Float] - r(4).asInstanceOf[Float])
.map(x => x*x)
.filter(!_.isNaN)
.reduce(_ + _)

predictions.take(10)

predictions.write.format("com.databricks.spark.csv").save("ml-predictions.csv")
