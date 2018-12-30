package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
  * Movie数据集，数据集字段通过分割
  *
  * 151^                          电影的ID
  * Rob Roy (1995)^               电影的名称
  * In the highlands ....^        电影的描述
  * 139 minutes^                  电影的时长
  * August 26, 1997^              电影的发行日期
  * 1995^                         电影的拍摄日期
  * English ^                     电影的语言
  * Action|Drama|Romance|War ^    电影的类型
  * Liam Neeson|Jessica Lange...  电影的演员
  * Michael Caton-Jones           电影的导演
  *
  * tag1|tag2|tag3|....           电影的Tag
  **/

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * MongoDB的连接配置
  *
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作数据库
  */
case class MongoConfig(uri: String, db: String)

//推荐
case class Recommendation(rid: Int, r: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val USER_MAX_RECOMMENDATION = 20

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  //入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "reommender"
    )

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
      .set("spark.executor.memory", "6G")
      .set("spark.driver.memory", "2G")

    //基于SparkConf创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //创建一个MongoDBConfig
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    //读取mongoDB中的业务数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)).cache()

    //创建训练数据集 RDD [ Rating(uid, mid, score) ]
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (50, 5, 0.01) //几个属性features, 迭代次数, 正则化参数
    /**训练ALS模型--------------------------------------------------------------------------*/
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    /**计算用户推荐矩阵--------------------------------------------------------------------------*/
    //所有用户的数据集 RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()
    //电影数据集 RDD[Int]
    val movieRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    //通过笛卡儿积 构造一个所有usersProducts  RDD[(Int,Int)]
    /**
    users:
        +-------+
        |userId|
        +-------+
        |   1   |
        |   2   |
        |   3   |
        +-------+

        movies：
        +---+
        |mid|
        +---+
        | 1 |
        | 2 |
        | 3 |
        +---+

        userMovies：
        +---------+
        |userId|mid|
        +---------+
        |  1  | 1 |
        |  1  | 2 |
        |  1  | 3 |
        |  2  | 1 |
        |  2  | 2 |
        |  2  | 3 |
          ... ...
        +---------+
      */
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

    // 预测集 RDD [ Rating(uid user, mid product, score rating) ]
    val preRatings: RDD[Rating] = model.predict(userMovies)
    /**
    preRatings：
        +---------+
        |uId  |mid| score
        +---------+
        |  1  | 1 |   ?
        |  1  | 2 |   ?
        |  1  | 3 |   ?
        |  2  | 1 |   ?
        |  2  | 2 |   ?
        |  2  | 3 |   ?
          ... ...
        +---------+*/
    val userRecs = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {case (uid, recs) =>             //按rating降序                //推荐20个
          UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    //用户推荐矩阵 存入 mongodb
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()



    /**计算电影相似度矩阵--------------------------------------------------------------------------*/
    //获取产品 (电影)的特征矩阵DoubleMatrix
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map { case (mid, freatures) =>
      (mid, new DoubleMatrix(freatures))
    }

    //电影和电影作比较 所有电影和电影笛卡儿积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      //过滤掉 元祖内 ._1 即mid相同的   不相同的true
      .filter { case (a, b) => a._1 != b._1 }
      .map { case (a, b) =>
        //计算两个电影之间的余弦相似度
        val simScore = this.consinSim(a._2, b._2)
        (a._1, (b._1, simScore))
      }.filter(_._2._2 > 0.6)// 余弦相似度>0.6
      .groupByKey()
      .map { case (mid, items) =>
        MovieRecs(mid, items.toList.map(x => Recommendation(x._1, x._2)))
      }.toDF()

    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //关闭Spark
    spark.close()
  }

  /**计算两个电影之间的余弦相似度--------------------------------------------------------------------------*/
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    //向量点乘 /
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}