package cmcc

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/2/18
  * Time: 15:17
  */
object CmccDemo1 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val groupId = "cmcc-abc"
    val kafkaParams: mutable.HashMap[String, Object] = new mutable.HashMap[String, Object]()
    kafkaParams += ("bootstrap.servers" -> "lu2:9092,lu3:9092,lu4:9092")
    kafkaParams += ("key.deserializer" -> classOf[StringDeserializer])
    kafkaParams += ("value.deserializer" -> classOf[StringDeserializer])
    //设置消费组id
    kafkaParams += ("group.id" -> groupId)
    //设置消费位置
    kafkaParams += ("auto.offset.reset" -> "earliest")
    // 设置不自动提交偏移量
    kafkaParams += ("enable.auto.commit" -> "false")
    val topic = "cmccTopic"
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //把数据均匀分配在每个executor上
      LocationStrategies.PreferConsistent,
      //订阅主题
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))
    val offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()
    val jedis1: Jedis = Utils("lu2", 6379)
    val sts: util.Map[String, String] = jedis1.hgetAll(groupId + "_" + topic)
    import scala.collection.JavaConversions._
    for (tp <- sts) {
      val part: Int = tp._1.toInt
      val offset: Long = tp._2.toLong
      //主题和分区
      val topicpart: TopicPartition = new TopicPartition(topic, part)
      offsetMap(topicpart) = offset
    }
    dstream.foreachRDD(rdd => {
      //获取当前批次的偏移量
      if (!rdd.isEmpty()) {
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //打印偏移量
        ranges.foreach(println)
        val jedis2: Jedis = Utils("lu2", 6379)
        ranges.foreach(of => {
          val gAndTopic = groupId + "-" + of.topic
          val part: Int = of.partition
          val offset: Long = of.untilOffset
          //将偏移量写入redis 必须指定值,不能白增
          jedis2.hset(gAndTopic, part + "", offset + "")
        })
        jedis2.close()
      }
    })

    dstream.foreachRDD(rdd => {
      val data: RDD[(String, List[Any])] = rdd.map(cr => JSON.parseObject(cr.value()))
        .filter(obj => obj.getString("serviceName").equals("reChargeNotifyReq"))
        .map(tp => {
          val bst: String = tp.getString("bussinessRst")
          val money: Double = tp.getString("chargefee").toDouble
          val id: String = tp.getString("requestId")
          val drt: String = tp.getString("receiveNotifyTime")
          val day: String = id.substring(0, 8)
          val hour: String = id.substring(8, 10)
          val minute: String = id.substring(10, 12)
          val time: Long = Utils.timeFormat(id, drt)
          //时接判读
          val flag: (Int, Double, Long) = if (bst.equals("0000")) (1, money, time) else (0, 0, 0)
          //天 数据次数 小时 分钟 成功标识 金额 时差
          (day, List(1, hour, minute, flag._1, flag._2, flag._3))
        })

    })
    jedis1.close()
    ssc.start()
    ssc.awaitTermination()
  }
}
