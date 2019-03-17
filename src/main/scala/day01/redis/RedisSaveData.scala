package day01.redis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by yjl
  * Data: 2019/2/17
  * Time: 23:40
  */
object RedisSaveData {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    //创建StreamingContext实例  实时 7*24小时
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    //读取socket数据,创建Dstream
    val ds1: ReceiverInputDStream[String] = ssc.socketTextStream("lu2", 9999)
    //接收器占用一个容器
    val ds2: DStream[(String, Int)] = ds1.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    ds2.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        //获取jedis连接
        val jedis: Jedis = RedisUtil("lu2", 6379)
        //      id:Interator[(String,Int)]
        it.foreach(tp => jedis.hincrBy("Streaming-wc",tp._1,tp._2))
        jedis.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
