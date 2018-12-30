package com.atguigu.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

object ConnHelper extends Serializable {
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop102:27017/recommender"))
}

case class MongConfig(uri: String, db: String)

//推荐
case class Recommendation(rid: Int, r: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_RATING_COLLECTION = "Rating"
  val MOVIE_RECS = "MovieRecs"
  val STREAM_RECS = "StreamRecs"

  //入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    //创建Spark的对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    implicit val mongConfig = MongConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._

    //******************  广播电影相似度矩阵
    //装换成为 Map[Int, Map[Int,Double]]  [电影, map[其他电影,相似度]]
    val simMoviesMatrix: collection.Map[Int, Map[Int, Double]] = spark
      .read
      .option("uri", config("mongo.uri"))
      .option("collection", MOVIE_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map { mapRecs =>
        (mapRecs.mid,
          mapRecs
            .recs
            .map(x => (x.rid, x.r))
            .toMap)//懂了
      }.collectAsMap()  //RDD转换成Map的方法

    //广播变量  电影相似度矩阵
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    val abc = sc.makeRDD(1 to 2)
    //广播变量懒加载所以要通过一个简单RDD action
    abc.map(x => simMoviesMatrixBroadCast.value.get(1)).count()

    //******************  kafka实时流处理部分
    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    // UID|MID|SCORE|TIMESTAMP
    // 产生实时评分流
    val ratingStream = kafkaStream.map { msg =>
      var attr = msg.value().split("\\|")
      // (UID,MID,SCORE,TIMESTAMP)
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    /** 针对实时传入的用户电影评分(uid,mid),实时推荐 */
    ratingStream.foreachRDD { rdd =>
      rdd.foreach { case (uid, mid, score, timestamp) =>
        println(">>>>>>>>>>>>>>>>")

        //获取当前最近的M次(20次)电影评分  //mid,score
        val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid)

        //获取电影P最相似的K个(20个)电影 且过滤掉已经看过得电影 //mid
        val simMovies: Array[Int] = getTopSimMovies(
          MAX_SIM_MOVIES_NUM,
          mid,
          uid,
          simMoviesMatrixBroadCast.value)

        //计算待选电影的推荐优先级
        // 传入1.电影相似度矩阵 2.最近20次评分的Array[(mid,score)] 3.电影P最相似的(20个)电影 Array[mid:Int]
        val streamRecs: Array[(Int, Double)] = computeMovieScores(
          simMoviesMatrixBroadCast.value,
          userRecentlyRatings,
          simMovies)

        //将数据保存到MongoDB
        saveRecsToMongoDB(uid, streamRecs)
      }
    }

    //启动Streaming程序
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取当前最近的M次电影评分
    *
    * @param num 评分的个数
    * @param uid 谁的评分
    * @return Array[(mid,score)]
    */
  def getUserRecentlyRating(num: Int, uid: Int): Array[(Int, Double)] = {

    val jedis = RedisUtil.pool.getResource
    //从用户的队列中取出num(20)个评论
    val list = jedis.lrange("uid:" + uid.toString, 0, num)

    jedis.close()

    list.map { item =>
      val attr = item.split("\\:")
      //mid , score
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取当前电影K个相似的电影
    *
    * @param num        相似电影的数量
    * @param mid        当前电影的ID
    * @param uid        当前的评分用户
    * @param simMovies  电影相似度矩阵的广播变量值
    * @param mongConfig MongoDB的配置
    * @return
    */
  def getTopSimMovies(
      num: Int,
      mid: Int,
      uid: Int,
      simMovies: collection.Map[Int, collection.immutable.Map[Int, Double]])
                     (implicit mongConfig: MongConfig): Array[Int] = {

    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影 mid,score
    val allSimMovies: Array[(Int, Double)] = simMovies(mid).toArray

    //获取用户已经观看过得电影
    val ratingExist = ConnHelper
      .mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map { item =>
        item.get("mid").toString.toInt
      }

    //过滤掉已经看过得电影，并按相似度排序输出 电影ID
    allSimMovies
      .filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
    * 计算待选电影的推荐分数
    *
    * @param simMovies           电影相似度矩阵
    * @param userRecentlyRatings 用户最近的k次评分
    * @param topSimMovies        当前电影最相似的K个电影 且过滤掉已经看过得电影
    * @return
    */
  def computeMovieScores(
      simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
      userRecentlyRatings: Array[(Int, Double)],
      topSimMovies: Array[Int]): Array[(Int, Double)] = {

    //用于保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //用于保存每一个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int, Int]()

    //用于保存每一个电影的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings) {
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)
      if (simScore > 0.6) {
        score += ((topSimMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie, 0) + 1
        } else {
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie, 0) + 1
        }
      }
    }

    score.groupBy(_._1).map { case (mid, sims) =>
      (mid, sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray

  }

  /**
    * 获取当个电影之间的相似度
    *
    * @param simMovies       电影相似度矩阵
    * @param userRatingMovie 用户已经评分的电影
    * @param topSimMovie     候选电影
    * @return
    */
  def getMoviesSimScore(
      simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
      userRatingMovie: Int,
      topSimMovie: Int): Double = {

    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  //取2的对数
  def log(m: Int): Double = {
    math.log(m) / math.log(2)
  }

  /**
    * 将数据保存到MongoDB    uid -> 1,  recs -> 22:4.5|45:3.8
    *
    * @param streamRecs 流式的推荐结果
    * @param mongConfig MongoDB的配置
    */
  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongConfig: MongConfig): Unit = {
    //到StreamRecs的连接
    val streaRecsCollection = ConnHelper.mongoClient(mongConfig.db)(STREAM_RECS)

    streaRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    streaRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => x._1 + ":" + x._2).mkString("|")))

  }

}
