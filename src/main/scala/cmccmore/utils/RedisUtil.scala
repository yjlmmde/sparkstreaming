package cmccmore.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by yjl
  * Data: 2019/2/21
  * Time: 17:25
  * 获取redis连接
  */
object RedisUtil {
   def getPool(host: String, port: Int) = {
    //连接池的配置参数
    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(2000)
    //最大连接数
    //连接池
    lazy val pool: JedisPool =new JedisPool(config, host, port)
    pool
  }
  def getJedis(host: String, port: Int) = getPool(host, port).getResource

}
