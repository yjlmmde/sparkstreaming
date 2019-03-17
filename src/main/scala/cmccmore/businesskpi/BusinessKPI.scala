package cmccmore.businesskpi

import java.util
import cmccmore.config.CmccConfig
import cmccmore.constant.CmccConstant
import cmccmore.utils.{RedisUtil, TimeTranforme}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import redis.clients.jedis.Jedis

/**
  * Created by yjl
  * Data: 2019/2/22
  * Time: 16:13
  * 具体的业务逻辑
  */
object BusinessKPI {
  /*数据的解析,过滤处理,匹配字段*/
  def makeBaseRDD(rdd: RDD[ConsumerRecord[String, String]]) = {
    val filterRDD: RDD[JSONObject] = rdd.map(cr => JSON.parseObject(cr.value()))
      /*过滤出充值通知类型的数据*/
      .filter(tp => tp.getString(CmccConstant.SERNAME.toString).equals(CmccConstant.RECHARGENR.toString))
    val baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))] =
    /*解析json数据 使用字段名取值根据需求 封装需要的所有字段*/
      filterRDD.map(tp => {
        val bussinessRst: String = tp.getString("bussinessRst")
        val requestId: String = tp.getString("requestId")
        val provinceCode: String = tp.getString("provinceCode")
        val day: String = requestId.substring(CmccConstant.DAYSTART, CmccConstant.DAYEND)
        val hour: String = requestId.substring(8, 10)
        val minute: String = requestId.substring(10, 12)
        /*判断 若是充值成功 就返回具体的金额,时长*/
        /*充值不成功,就返回 具体的 0 值,方便需求处理*/
        val flag: (Int, Int, Double, Long) = if (bussinessRst.equals("0000")) {
          val chargefee: Double = tp.getString("chargefee").toDouble
          val NotifyTime: String = tp.getString("receiveNotifyTime")
          val startTime: String = requestId.substring(0, 17)
          val time: Long = TimeTranforme.getTime(startTime, NotifyTime)
          /* 数据条数 订单数据成功为1失败为0 充值金额  充值时长*/
          (1, 1, chargefee, time)
        }
        else {
          (1, 0, 0.0D, 0L)
        }
        /*天 小时 分钟 省份编码 标识(数据标识 成功标识 充值金额 充值时长 )*/
        (day, hour, minute, provinceCode, flag)
      }).cache() //缓存一下
    baseRDD
  }

  /*按天统计充值数据总量,充值成功总量,充值成功率,充值平均时长*/
  def kpi_daily(baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    baseRDD.map(tp => (tp._1, tp._5)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
      .foreachPartition(it => {
        val jedis: Jedis = RedisUtil.getJedis(CmccConfig.host, CmccConfig.port)
        it.foreach {
          case (day, (total, success, money, time)) => {
            /*将充值数据存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":day:" + day, "total", total)
            /*将充值成功的数据存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":day:" + day, "success", success)
            /*将充值金额存入reids*/
            jedis.hincrByFloat(CmccConstant.PROJECT + ":day:" + day, "money", money)
            /*将充值时长存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":day:" + day, "time", time)
          }
        }
        jedis.close()
      })
  }

  /** 业务概况  -- 每小时成功订单数量  成功率 = 成功订单数量 /  总的数量 */
  /* day  hour    数据标识  充值成功标识 */
  def kpi_hour(baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    baseRDD.map(tp => ((tp._1, tp._2), (tp._5._1, tp._5._2))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(it => {
        val jedis: Jedis = RedisUtil.getJedis(CmccConfig.host, CmccConfig.port)
        it.foreach {
          case ((day, hour), (total, success)) => {
            /*将每小时充值数据存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":hour:" + day, "total-" + hour, total)
            /*将每小时充值成功的数据存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":hour:" + day, "success-" + hour, success)
          }
        }
        jedis.close()
      })
  }

  /*每分钟的处理 总的充值订单  成功的充值订单*/
  def kpi_minute(baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    baseRDD.map(tp => ((tp._1, tp._2, tp._3), (tp._5._2, tp._5._3))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(it => {
        val jedis: Jedis = RedisUtil.getJedis(CmccConfig.host, CmccConfig.port)
        it.foreach {
          case ((day, hour, minute), (success, money)) => {
            /*将每分钟的 充值订单存入redis*/
            jedis.hincrByFloat(CmccConstant.PROJECT + ":minute:" + day, "money-" + hour + minute, money)
            /*将每分钟的 充值成功订单存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":minute:" + day, "success-" + hour + minute, success)
          }
        }
        jedis.close()
      })
  }

  /*按天对各省份的充值统计*/
  def kpi_province_daily(ssc: StreamingContext, baseRDD: RDD[(String, String, String, String, (Int, Int, Double, Long))]) = {
    /*将省份编码数据广播出去*/
    val bcProvince: Broadcast[util.Map[String, AnyRef]] = ssc.sparkContext.broadcast(CmccConfig.code2Province)
    /*按天对各省份的充值统计*/
    baseRDD.map(tp => ((tp._1, tp._4), (tp._5._1, tp._5._2))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .foreachPartition(it => {
        val jedis: Jedis = RedisUtil.getJedis(CmccConfig.host, CmccConfig.port)
        it.foreach {
          case ((day, provinceCode), (total, success)) => {
            /*通过key值获取省份value值*/
            val province: String = bcProvince.value.get(provinceCode).asInstanceOf[String]
            /*将每天的省份充值数据存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":pro-total:" + day, province, total)
            /*将每天的省份充值成功的数据存入redis*/
            jedis.hincrBy(CmccConstant.PROJECT + ":pro-success:" + day, province, success)
            /*使用sortedSet数据存数据,有序*/
            jedis.zincrby(CmccConstant.PROJECT + ":pro-total-zsort" + day, total, province)
            jedis.zincrby(CmccConstant.PROJECT + ":pro-success-zsort" + day, success, province)
          }
        }
        jedis.close()
      })
  }
}
