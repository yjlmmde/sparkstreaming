package cmcc

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}

/**
  * Created by yjl
  * Data: 2019/2/21
  * Time: 14:25
  */
object Test {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()
    val configObject: ConfigObject = config.getObject("code2Province")
    import scala.collection.JavaConversions._
    val map: Map[String, AnyRef] = configObject.unwrapped().toMap
    map.foreach(println)
  }

}
