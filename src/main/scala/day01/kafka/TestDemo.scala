package day01.kafka
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Created by yjl
  * Data: 2019/2/16
  * Time: 22:07
  */
object TestDemo {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","lu2:9092,lu3:9092,lu4:9092")
    // 反序列化
    prop.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    // 设置消费组id
    prop.setProperty("group.id","abc4")


    // 设置消费的位置 默认从最新位置开始消费
    // [latest, earliest, none]
    prop.setProperty("auto.offset.reset","earliest")

    //  offset的维护，默认是true  kafka自己管理偏移量的提交
    prop.setProperty("enable.auto.commit","false")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](prop)

    while (true) {
      // 订阅主题
      consumer.subscribe(util.Arrays.asList("testTopic1"))
      // 获取数据
      val data: ConsumerRecords[String, String] = consumer.poll(2000)
      val records: util.Iterator[ConsumerRecord[String, String]] = data.iterator()

      // 迭代数据
      import scala.collection.JavaConversions._
      for (cr <- records) {
        println(cr)
      }
    }
  }

}
