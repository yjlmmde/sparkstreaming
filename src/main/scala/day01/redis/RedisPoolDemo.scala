package day01.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by yjl
  * Data: 2019/2/17
  * Time: 22:51
  */
object RedisPoolDemo {
  def main(args: Array[String]): Unit = {
    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(2000)
    //最大连接数
    val pool: JedisPool = new JedisPool(config,"lu2",6379)
    //从连接池中获取连接
    val jedis1: Jedis = pool.getResource
    val jedis2: Jedis = pool.getResource
    println(jedis1.ping())
    jedis1.close()

  }
}
