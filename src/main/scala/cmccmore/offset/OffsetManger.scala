package cmccmore.offset

import java.util

import cmccmore.config.CmccConfig
import cmccmore.utils.RedisUtil
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/2/22
  * Time: 16:36
  */
object OffsetManger {
  def getOffsetMap = {
    val offsetMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()
    val jedis: Jedis = RedisUtil.getJedis(CmccConfig.host, CmccConfig.port)
    val sts: util.Map[String, String] = jedis.hgetAll(CmccConfig.groupId + "_" + CmccConfig.topic)
    import scala.collection.JavaConversions._
    for (tp <- sts) {
      val part: Int = tp._1.toInt
      val offset: Long = tp._2.toLong
      //主题和分区
      val topicpart: TopicPartition = new TopicPartition(CmccConfig.topic, part)
      offsetMap(topicpart) = offset
    }
    offsetMap
  }
  def setOffsetToRedis(ranges: Array[OffsetRange]) = {
    val jedis: Jedis = RedisUtil.getJedis(CmccConfig.host, CmccConfig.port)
    ranges.foreach(of => {
      val gAndTopic = CmccConfig.groupId + "-" + of.topic
      val part: Int = of.partition
      val offset: Long = of.untilOffset
      //将偏移量写入redis 必须指定值,不能白增
      jedis.hset(gAndTopic, part + "", offset + "")
    })
    jedis.close()
  }
}
