package day01.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}

/**
  * Created by yjl
  * Data: 2019/2/16
  * Time: 20:57
  */
object KafkaDemo1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers","lu2:9092,lu3:9092,lu4:9092")
    prop.setProperty("key.serializer",classOf[StringSerializer].getName)
    prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    //生产者
    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](prop)
    //消息
    //一般情况下,value多指定为String类型
    for (i <- 0 to 10000) {
      val part= i%3
      val mes: ProducerRecord[String, String] = new ProducerRecord[String, String]("testTopic1", part, "", i+"")
//      new ProducerRecord[String,String]("topic1","")
      producer.send(mes)
    }
    /*
    * 如果指定分区编号,就按照分区编号来执行
    * 如果没有指定分区编号,但制定了key值,就按照key的hash 来进行分区
    * 如果既没有指定分区也没有指定key,就按照轮询的方式 0 1 2 0 1 2
    *
    * */
    producer.close( )
  }
}
