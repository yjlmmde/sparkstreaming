package cmcc

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.{Jedis}

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/2/20
  * Time: 17:19
  */
object CmccDemo01 {
    Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val groupId = "cmcc-abc2"
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
    //获取sc实例
    val sc: SparkContext = ssc.sparkContext
    //读取省份规则文件
    val pvdata: RDD[String] = sc.textFile("province.txt")
    val pvrdd: RDD[(String, String)] = pvdata.map(tp => {
      //根据空格切分
      val arr: Array[String] = tp.split(" ")
      (arr(0), arr(1))
    })
    //    pvrdd.foreach(println)
      val config: Properties = new Properties()
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
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //把数据均匀分配在每个executor上
      LocationStrategies.PreferConsistent,
      //订阅主题
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams,offsetMap))

    dstream.foreachRDD(rdd => {

    })

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
      val basedata: RDD[JSONObject] = rdd.map(cr => JSON.parseObject(cr.value()))
        .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")).cache()
      val pcmdata: RDD[(String, (Int,String, Int))] = basedata.map(tp => {
        val bst: String = tp.getString("bussinessRst")
        val id: String = tp.getString("requestId")
        val drt: String = tp.getString("receiveNotifyTime")
        val provinceCode: String = tp.getString("provinceCode")
        val day: String = id.substring(0, 8)
        val hour: String = id.substring(8, 10)
        val minute: String = id.substring(10, 12)
        val flag: Int = if (bst.equals("0000")) 1 else 0
         //省份    //订单条数 天  标识
        (provinceCode, (1,day, flag))
      })
//      val dddd: RDD[(String, ((Int, String, Int), String))] = pcmdata.join(pvrdd)
      //充值成功的订单                                                         //day+省份          //订单flag
      val allpaydata: RDD[(String, Int)] = pcmdata.join(pvrdd).map(tp => (tp._2._1._2 + tp._2._2, tp._2._1._1)).reduceByKey(_ + _)
      //所有充值订单                                                    //day+省份          //订单条数1
      val succdata: RDD[(String, Int)] = pcmdata.join(pvrdd).map(tp =>(tp._2._1._2+tp._2._2,tp._2._1._3)).reduceByKey(_+_)
      //所有pay订单
      allpaydata.foreachPartition(it => {
        //获取redis连接池连接
        val jedis = Utils("lu2", 6379)
        it.foreach(tmp => {
          //将数据按省份存入redis  使用sortedset类型
          jedis.zincrby("cmcc-province-allrecharge", tmp._2, tmp._1)
        })
        jedis.close()
      })
      //success  recharge数据
      succdata.foreachPartition(it =>{
        val jedis:Jedis = Utils("lu2",6379)
        it.foreach(tmp =>{
          jedis.zincrby("cmcc-province-succ",tmp._2,tmp._1)

        })
        jedis.close()
      })

     /* val jedis = Utils("lu2", 6379)
      //取值前十条
      val top10: util.Set[Tuple] = jedis.zrevrangeWithScores("cmcc-province-allrecharge", 0, 9)
      //导包java 转scala
      import scala.collection.JavaConversions._
      //循环打印top10
      for (tup <- top10 ) {
        System.out.println(tup.getElement + ":" + tup.getScore)
      }
      jedis.close()*/
      }
    })

    jedis1.close()
    ssc.start()
    ssc.awaitTermination()
  }
}
