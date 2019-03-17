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
object CmccDemo {
    Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val groupId = "cmcc-abc"
    val topic = "cmccTopic"
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
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //把数据均匀分配在每个executor上
      LocationStrategies.PreferConsistent,
      //订阅主题
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams,offsetMap))

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
      val resource: RDD[(String, String, String, String, String)] = rdd.map(tp => {
        val data: JSONObject = JSON.parseObject(tp.value())
        val bst: String = data.getString("bussinessRst")
        val money: String = data.getString("chargefee")
        val id: String = data.getString("requestId")
        val sn: String = data.getString("serviceName")
        val drt: String = data.getString("receiveNotifyTime")
        //订单id,接口标识,业务结果,充值金额,充值通知时间
        (id, sn, bst, money, drt)
      })
      //充值数据
      val data: RDD[(String, String, String, String, String)] = resource.filter(tp => tp._2.equals("reChargeNotifyReq"))
      //充值成功数据
      val succdata: RDD[(String, String, String, String, String)] = data.filter(tp => tp._3.equals("0000"))
      //succdata 规则字段数据
      val ruledata: RDD[(String, String, String, String, String, String)] = succdata.map(tp => {
        val day: String = tp._1.substring(0, 8)
        val hour = tp._1.substring(8, 10)
        val minute = tp._1.substring(10, 12)
        val id: String = tp._1
        val money: String = tp._4
        val drt: String = tp._5
        //年月日,时,订单id,充值金额,充值通知时间
        (day, hour, minute, id, money, drt)
      }).cache()
      //充值数据
      val rechargetotl: RDD[(String, Int)] = data.map(tp => (tp._1.substring(0, 8), 1)).reduceByKey(_ + _)
      //充值成功的条数
      val succtotl: RDD[(String, Int)] = ruledata.map(tp => (tp._1, 1)).reduceByKey(_ + _)
      //统计成功支付金额
      val paymdata: RDD[(String, Double)] = ruledata.map(tp => (tp._1, tp._5.toDouble)).reduceByKey(_ + _)
      //平均时间
      val avgtdata: RDD[(String, Long)] = ruledata.map(tp => {
        //开始时间
        val start: String = tp._4.substring(0, 17)
        //结束时间
        val end: String = tp._6
        //时间处理工具  返回时间差
        val time: Long = Utils.timeFormat(start, end)
        (tp._1, time)
      }).reduceByKey(_ + _)
      //实时业务
      //充值数据
      val real_rechartotl: RDD[(String, Int)] = data.map(tp => (tp._1.substring(0, 10), 1)).reduceByKey(_ + _)
      //充值成功数据
      val real_succtotl: RDD[(String, Int)] = ruledata.map(tp => (tp._1 + tp._2, 1)).reduceByKey(_ + _)
      //实时按分钟分析  充值成功的订单
      //充值金额                                                // 秒           //充值金额
      val mmatat: RDD[(String, Double)] = ruledata.map(tp =>(tp._1+tp._2+tp._3,tp._5.toDouble)).reduceByKey(_+_)
      //充值成功条数数据                                        数据条数1
      val malldata: RDD[(String, Int)] = ruledata.map(tp =>(tp._1+tp._2+tp._3,1)).reduceByKey(_+_)


      //按分钟将minute-all数据存入redis
      mmatat.foreachPartition(it =>{
        val jedis: Jedis = Utils("lu2",6379)
        it.foreach(tmp=>{
          //将minute-all数据存入redis
          jedis.hincrByFloat("cmcc-minute-all",tmp._1,tmp._2)
        })
        jedis.close()
      })
      //按分钟将minute-money数据存入redis
      malldata.foreachPartition(it=>{
        val jedis: Jedis = Utils("lu2",6379)
        it.foreach(tmp=>{
          //将minute-money数据存入redis
          jedis.hincrBy("cmcc-minute-money",tmp._1,tmp._2)
        })
      })
      //实时  将 all充值数据 存入redis
      real_rechartotl.foreachPartition(it => {
        val jedis = Utils("lu2", 6379)
        it.foreach(tmp => {
          //将 实时的 充值数据 存入redis
          jedis.hincrBy("cmcchour-rectotl", tmp._1, tmp._2)
        })
        jedis.close()
      })
      //实时将  充值成功  数据all存入redis
      real_succtotl.foreachPartition(it => {
        val jedis: Jedis = Utils("lu2", 6379)
        it.foreach(tmp => {
          //将实时的 充值成功 的数据存入redis
          jedis.hincrBy("cmcchour-succtotl", tmp._1, tmp._2)
          //从redis获取 实时的  充值成功  的数据
          val succtotl: Long = jedis.hget("cmcchour-succtotl", tmp._1).toLong
          //从redis获取 实时的  充值  的数据
          val rectotl: Long = jedis.hget("cmcchour-rectotl", tmp._1).toLong
          //将 实时的  充公率 存入 redis
          jedis.hset("cmcchour-rate", tmp._1, (succtotl * 1.0 / rectotl) + "")
        })
        jedis.close()
      })

      //按天处理数据
      //统计按天所有充值数据 存入redis
      rechargetotl.foreachPartition(it => {
        val jedis: Jedis = Utils("lu2", 6379)
        it.foreach(tmp => {
          //按天所有充值数据存入redis
          jedis.hincrBy("cmcc-all", tmp._1, tmp._2)
        })
        jedis.close()
      })
      //按天统计支付成功的条数 并存入redis
      succtotl.foreachPartition(it => {
        val jedis: Jedis = Utils("lu2", 6379)
        it.foreach(tmp => {
          //将充值成功的订单存入redis
          jedis.hincrBy("cmcc-succ", tmp._1, tmp._2)
          //获取所有充值订单数量
          val all: Long = jedis.hget("cmcc-all", tmp._1).toLong
          //获取充值成功的订单数量
          val succ: Long = jedis.hget("cmcc-succ", tmp._1).toLong
          //支付成功率
          jedis.hincrByFloat("cmcc-rate", tmp._1, succ * 1.0 / all)
        })
        jedis.close()
      })
      //按天统计成功支付金额
      paymdata.foreachPartition(it => {
        val jedis: Jedis = Utils("lu2", 6379)
        it.foreach(tmp => {
          //按天将统计成功支付金额存入redis
          jedis.hincrByFloat("cmcc-succpay", tmp._1, tmp._2)
        })
        jedis.close()
      })
      //年月日,时,订单id,接口标识,业务结果,充值金额,充值通知时间
      //按天将充值平均时长存入redis
      avgtdata.foreachPartition(it => {
        val jedis: Jedis = Utils("lu2", 6379)
        it.foreach(tp => {
          //按天将时间差存入redis
          jedis.hincrBy("cmcc-alltime", tp._1, tp._2)
          //从redis获取所有时间差
          val time = jedis.hget("cmcc-alltime", tp._1).toLong
          //从redis 按天获取充值成功的订单量
          val counts: Long = jedis.hget("cmcc-succ", tp._1).toLong
          val avg: Double = time * 1.0 / counts
          //平均时长
          jedis.hincrByFloat("cmcc-avgtime", tp._1, avg)
        })
        jedis.close()
      })
    })
    jedis1.close()
    ssc.start()
    ssc.awaitTermination()
  }
}
