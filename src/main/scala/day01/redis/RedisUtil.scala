package day01.redis

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by yjl
  * Data: 2019/2/17
  * Time: 22:56
  */
object RedisUtil {
  private def getPool(host:String,port:Int)={
    //连接池的配置参数
    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(2000)
    //最大连接数
    //连接池
    val pool: JedisPool = new JedisPool(config,host,port)
    pool
  }
  def apply(host:String,port:Int)= getPool(host,port).getResource
}
