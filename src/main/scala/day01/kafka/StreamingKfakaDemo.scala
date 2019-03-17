package day01.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/2/16
  * Time: 23:35
  */
object StreamingKfakaDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val kafkaParams = new mutable.HashMap[String, Object]()
    kafkaParams += ("bootstrap.servers" -> "lu2:9092,lu3:9092,lu4:9092")
    kafkaParams += ("key.deserializer" -> classOf[StringDeserializer].getName)
    kafkaParams += ("value.deserializer" -> classOf[StringDeserializer].getName)
    // 设置消费组id
    kafkaParams += ("group.id" -> "abc7")
    // [latest, earliest, none]
    // 设置消费的位置 默认从最新位置开始消费
    kafkaParams += ("auto.offset.reset" -> "earliest")
    //    kafkaParams += ("enable.auto.commit","false")
    val topic="testTopic1"
    //创建StreamingContext实例  实时 7*24小时可用
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      // 把数据，均匀的分布在各个executor中
      LocationStrategies.PreferConsistent,
      // 订阅主题
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams: collection.Map[String, Object]))
    dstream.foreachRDD(rdd=>{
      rdd.foreach(println)
      // 业务逻辑
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
