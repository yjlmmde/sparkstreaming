package cmccmore

import java.util

import cmccmore.businesskpi.BusinessKPI
import cmccmore.config.CmccConfig
import cmccmore.offset.OffsetManger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

/**
  * Created by yjl
  * Data: 2019/2/21
  * Time: 14:32
  *
  */
object CmccMain {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    /*获取StreamContext对象*/
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(CmccConfig.batch_Time))
    /*获取消费对象*/
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //把数据均匀分配在每个executor上
      LocationStrategies.PreferConsistent,
      //订阅主题
      ConsumerStrategies.Subscribe[String, String](Array(CmccConfig.topic), CmccConfig.kafkaParams,OffsetManger.getOffsetMap))
    //从redis中获取偏移量
    dstream.foreachRDD(rdd => {
      //获取当前批次的偏移量
      if (!rdd.isEmpty()) {
        //获取偏移量
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //打印偏移量
        ranges.foreach(println)
        val baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))] = BusinessKPI.makeBaseRDD(rdd)
        /*按天统计充值数据总量,充值成功总量,充值成功率,充值平均时长*/
        BusinessKPI.kpi_daily(baseRDD)
        /** 业务概况  -- 每小时成功订单数量  成功率 = 成功订单数量 /  总的数量 */
        /* day  hour    数据标识  充值成功标识 */
        BusinessKPI.kpi_hour(baseRDD)
        /*每分钟的处理 总的充值订单  成功的充值订单*/
        BusinessKPI.kpi_minute(baseRDD)
        /*按天对各省份的充值统计*/
        BusinessKPI.kpi_province_daily(ssc, baseRDD)
        /*将偏移量记录到到redis中*/
        OffsetManger.setOffsetToRedis(ranges)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
