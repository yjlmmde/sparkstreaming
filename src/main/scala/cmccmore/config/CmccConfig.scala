package cmccmore.config

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/2/22
  * Time: 15:45
  */
object CmccConfig {
  /*获取读取配置文件的对象*/
  /*懒加载*/
  private lazy val config: Config = ConfigFactory.load()
  /*主题*/
  val topic: String = config.getString("kafka.topic")
  /*消费组*/
  val groupId: String = config.getString("kafka.groupid")
  /*broker节点信息*/
  val brokerid: String = config.getString("kafka.brokerid")
  val auto_Offset: String = config.getString("kafka.auto.offset")
  val enable_Auto: String = config.getString("kafka.enable.auto")
  /*streaming 程序的批次时间*/
  val batch_Time: Int = config.getInt("streaming.batch.time")
  val host: String = config.getString("redis.host")
  val port: Int = config.getInt("redis.port")
  /*创建kafka参数集合*/
  import scala.collection.JavaConversions._
  /* kafka配置参数 */
  val kafkaParams: Map[String, Object] = Map[String, Object](
    /*broker节点 ip:端口号*/
    "bootstrap.servers" -> brokerid,
    /*反序列化*/
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    /*设置消费组id*/
    "group.id" -> groupId,
    /*设读取kafka数据的位置*/
    "auto.offset.reset" -> auto_Offset,
    /*设置是否自动维护偏移量*/
    "enable.auto.commit" -> enable_Auto
  )
  /*读取配置文件中的 省份编码对应*/
  val code2Province: util.Map[String, AnyRef] = config.getObject("code2Province").unwrapped()

}
