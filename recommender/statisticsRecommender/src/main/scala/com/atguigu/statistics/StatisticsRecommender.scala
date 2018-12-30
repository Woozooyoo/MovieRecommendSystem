package com.atguigu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
case class Movie( mid: Int,  name: String,  descri: String,  timelong: String,  issue: String,
                  shoot: String,  language: String,  genres: String,  actors: String,  directors: String)

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
case class Rating( uid: Int,  mid: Int,  score: Double,  timestamp: Int)

/**
  * MongoDB的连接配置
  * @param uri   MongoDB的连接
  * @param db    MongoDB要操作数据库
  */
case class MongoConfig( uri:String,  db:String)

/**
  * 推荐对象
  * @param rid   推荐的Movie的mid
  * @param r     Movie的评分
  */
case class Recommendation(rid:Int, r:Double)

/**
  * 电影类别的推荐
  * @param genres   电影的类别
  * @param recs     top10的电影的集合
  */
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计的表的名称
  val AVERAGE_MOVIES = "AverageMovies"
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val GENRES_TOP10_MOVIES = "GenresTop10Movies"
  val QUARTER_TOP_MOVIES ="QuarterTopMovies"
  val HIGH_QUALITY_MOVIES="HighQualityMovies"

  // 入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    /*"mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"*/
    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //加入隐式转换
    import spark.implicits._
    //数据加载进来
    val ratingDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建ratings movies表
    ratingDF.createOrReplaceTempView("ratings")
    movieDF.createOrReplaceTempView("movies")

    /** 需求一: 统计所有历史数据中每个电影的评分个数 simple*/

    //数据结构 -》  mid,count
    val rateMoreMoviesDF = spark.sql(
      s"""
         |SELECT
         |    MID, COUNT(MID) AS COUNT
         |FROM
         |    ratings
         |GROUP BY MID
       """.stripMargin)

    rateMoreMoviesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /** 需求二: 统计以月为单位 每个电影的评分个数*/
    //数据结构 -》 mid,count,time

    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个UDF函数，用于将timestamp装换成年月格式   1260759144000  => 201605
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format( new Date(x * 1000L)).toInt )

    // 将原来的Rating数据集中的时间转换成年月的格式
    val ratingOfYeahMouth = spark.sql("select mid, score, changeDate(timestamp) as yearMouth from ratings")
    // 将新的数据集注册成为一张表
    ratingOfYeahMouth.createOrReplaceTempView("ratingOfMouth")

    val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count ,yearMouth from ratingOfMouth group by yearMouth,mid")

    rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /** 需求三: 统计每个电影的平均评分  simple*/

    spark.udf.register("halfUp",(num:Double, scale:Int) => {
      val bd = BigDecimal(num)
      bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    })

    val averageMoviesDF = spark.sql(
      s"""
         |SELECT
         |    MID, halfUp (AVG(score), 2) AvgScore
         |FROM
         |    ratings
         |GROUP BY MID
       """.stripMargin)

    averageMoviesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /** 需求四: 统计每种电影类型中评分最高的10个电影*/
       //需要用left join，应为只需要有评分的电影数据集
    val movieWithScore = movieDF.join(averageMoviesDF,Seq("mid","mid"))
    movieWithScore.createTempView("movieWithScore")

    //!!!!! 通过explode来实现 计算电影类别top10
    import org.apache.spark.sql.functions._
    spark.udf.register("splitGe", (x:String) => x.split("\\|"))

    val singleGenresMovie = movieWithScore.select($"mid", $"AvgScore", explode(callUDF("splitGe",$"genres")).as("gen"))
    singleGenresMovie.show
    /*+----+------------------+--------+
      | mid|               avg|     gen|
      +----+------------------+--------+
      | 148|               4.0|   Drama|
      | 463|3.4285714285714284|   Crime| 463同一个电影炸开成三种类别
      | 463|3.4285714285714284|   Drama| 463
      | 463|3.4285714285714284|Thriller| 463
      +----+------------------+--------+*/
    /** 类别Top10电影统计---DSL------------------------------*/
    val genresTopMovies = singleGenresMovie.rdd
      .map(row => (row.getAs[String]("gen"),  (row.getAs[Int]("mid"), row.getAs[Double]("avg"))  ))
      .groupByKey()
      .map{
        case (genres, items) =>
          GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1,item._2)))
      }.toDF()

    /** 类别Top10电影统计---SQL------------------------------*/
    val genresTop10Movies = spark.sql(
      s"""
         |SELECT
         |    *
         |FROM
         |    (SELECT
         |        MID, gen, AvgScore, row_number () over (PARTITION BY gen
         |    ORDER BY AvgScore DESC) rank
         |    FROM
         |        (SELECT
         |            MID, AvgScore, explode (splitGe (genres)) gen
         |        FROM
         |            movieWithScore) genresMovies) rankGenresMovies
         |WHERE rank <= 10
       """.stripMargin)

    // 输出数据到MongoDB
    genresTop10Movies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",GENRES_TOP10_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /**统计优质电影---------------------------------*/
    val HighQualityMoviesDF = spark.sql(
      s"""
         |SELECT
         |    average.mid, halfUp (average.avg, 2) AVG, average.count,
         |    row_number () over (
         |    ORDER BY average.avg DESC, average.count DESC
         |    ) rank
         |FROM
         |    (SELECT
         |        MID, AVG(score) AVG, COUNT(*) COUNT
         |    FROM
         |        ratings
         |    GROUP BY MID) average
         |WHERE AVG > 3.5
         |    AND COUNT> 50
       """.stripMargin)

    // 将统计结果写入MongoDB
    HighQualityMoviesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",HIGH_QUALITY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /** 最近一季度(90天)内的TOP10热门电影统计---------------------------------*/
    val quarterTopMovies = spark.sql(
      s"""
         |SELECT
         |    MID, COUNT(*) COUNT
         |FROM
         |    ratings
         |WHERE TIMESTAMP >
         |    (SELECT
         |        MAX(TIMESTAMP) - 7776000 MAX
         |    FROM
         |        ratings)
         |GROUP BY MID
         |ORDER BY count desc
         |LIMIT 10
       """.stripMargin)

    // 将统计结果写入MongoDB
    quarterTopMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",QUARTER_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //关闭Spark
    spark.stop()
  }

}
