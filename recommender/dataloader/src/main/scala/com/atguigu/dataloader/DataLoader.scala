package com.atguigu.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

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
  * Tag数据集，用户对于电影的标签数据集，用，分割
  *
  * 15,          用户的ID
  * 1955,        电影的ID
  * dentist,     标签的具体内容
  * 1193435061   用户对于电影打标签的时间
  */
case class Tag( uid: Int,  mid: Int,  tag: String,  timestamp: Int)

/**
  * MongoDB的连接配置
  * @param uri   MongoDB的连接
  * @param db    MongoDB要操作数据库
  */
case class MongoConfig( uri:String,  db:String)

/**
  * ElasticSearch的连接配置
  * @param httpHosts       Http的主机列表，以，分割
  * @param transportHosts  Transport主机列表， 以，分割
  * @param index           需要操作的索引
  * @param clusterName     ES集群的名称，
  */
case class ESConfig( httpHosts:String,  transportHosts:String,  index:String,  clusterName:String)

// 数据的主加载服务
object DataLoader {

  val MOVIE_DATA_PATH = "E:\\Study\\BDpan\\003-尚硅谷大数据集合\\尚硅谷大数据2017-201804\\26_电影推荐系统13-23\\4.video\\day02 230\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "E:\\Study\\BDpan\\003-尚硅谷大数据集合\\尚硅谷大数据2017-201804\\26_电影推荐系统13-23\\4.video\\day02 230\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "E:\\Study\\BDpan\\003-尚硅谷大数据集合\\尚硅谷大数据2017-201804\\26_电影推荐系统13-23\\4.video\\day02 230\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "hadoop102:9200",/*,hadoop103,hadoop104*/
      "es.transportHosts" -> "hadoop102:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )
    // 需要创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 将Movie、Rating、Tag数据集加载进来
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
      //将MovieRDD装换为DataFrame
    val movieDF = movieRDD.map(item =>{
      val attr = item.split("\\^")
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
      //将ratingRDD转换为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
      //将tagRDD装换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()

     /**需要将数据保存到MongoDB中*/
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //storeDataInMongoDB(movieDF, ratingDF, tagDF)


    /**
      * 插入 ElasticSearch
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

    // 首先需要将Tag数据集进行处理，  处理后的形式为  MID ， tag1|tag2|tag3     tag1   tag2  tag3
    /**
      *  MID , Tags
      *  1     tag1|tag2|tag3|tag4....
      */
    import org.apache.spark.sql.functions._ //concat_ws
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags")).select("mid","tags")

    // 需要将处理后的Tag数据，和Moive数据融合，产生新的Movie数据，  没有tags的电影也需要 所以用leftjoin
    val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left")

    // 声明了一个ES配置的隐式参数
    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    /**需要将新的Movie数据保存到ES中*/
    storeDataInES(movieWithTagsDF)

    // 关闭Spark
    spark.stop()
  }

  // 将数据保存到MongoDB中的方法
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF:DataFrame, tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    /*"mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"*/

    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果MongoDB中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到MongoDB
    movieDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    //关闭MongoDB的连接
    mongoClient.close()
  }

  // 将数据保存到ES中的方法
  def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig): Unit = {

    //新建一个配置
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clusterName).build()

    //新建一个ES的客户端
    val esClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient中 按,切分不同的node  按正则切分hn和port
    val REGEX_HOST_PORT = "(.+):(\\d+)".r //(.+)有任意一个字符 ,字符个数是一到多  加上符号 :  (\\d+)有任意一个数字,数字个数是一到多
    eSConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
    }

    /*"es.httpHosts" -> "hadoop102:9200",/*,hadoop103,hadoop104*/
      "es.transportHosts" -> "hadoop102:9300,?,?",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"*/

    //需要清除掉ES中遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    //将数据写入到ES中
    movieDF
      .write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid") //id 和"mid"的内容相同
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX) //index+type
  }
}
