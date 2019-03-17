package cmcc

import java.text.SimpleDateFormat

import org.apache.commons.lang.time.FastDateFormat
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by yjl
  * Data: 2019/2/18
  * Time: 15:29
  */
object Utils {
  private def getPool(host: String, port: Int) = {
    //连接池的配置参数
    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(2000)
    //最大连接数
    //连接池
    val pool = new JedisPool(config, host, port)
    val jedis = pool.getResource
    pool
  }
  def apply(host: String, port: Int) = getPool(host, port).getResource

  def timeFormat(start: String, end: String) = {
    //线程不安全
    val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS")
    val time: Long = format.parse(end).getTime() - format.parse(start).getTime
    time
  }
}
