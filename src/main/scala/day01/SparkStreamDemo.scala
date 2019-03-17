package day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yjl
  * Data: 2019/2/14
  * Time: 11:33
  */
object SparkStreamDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    //读取socket数据,创焕DStream
    val ds1: ReceiverInputDStream[String] = ssc.socketTextStream("lu2",9999)
    val ds2: DStream[(String, Int)] = ds1.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //action算子
    ds2.print()
    //启动reciver
    ssc.start()
    //挂起阻塞保证一直开启
    ssc.awaitTermination()
  }

}
