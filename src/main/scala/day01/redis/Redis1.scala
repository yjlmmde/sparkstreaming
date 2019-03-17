package day01.redis

import java.util

import org.apache.log4j.{Level, Logger}
import redis.clients.jedis.Jedis

/**
  * Created by yjl
  * Data: 2019/2/17
  * Time: 22:41
  */
object Redis1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = new Jedis("lu2",6379)
    println(jedis.ping())
    //String类型
    jedis.set("num:999","666")
    println(jedis.get("num:999"))
    //hash类型
    jedis.hset("bj","四环",123+"")
    jedis.hset("bj","五环",231+"")
    val hv: util.Map[String, String] = jedis.hgetAll("bj")
    import scala.collection.JavaConversions._
    for ( (fk,fv) <- hv ){
      println(fk,fv)
    }
    //自增
    jedis.hincrBy("bj","五环",10)
    jedis.close()
  }
}
