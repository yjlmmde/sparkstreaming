import java.text.SimpleDateFormat

import cmcc.Utils
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.spark.rdd.RDD
import org.codehaus.jackson.map.MapperConfig.ConfigFeature

val sss="20170412030007090581518228485394"
val lss=sss.length
val ss="20170412030030017"
val sub=ss.substring(0,14)
val day=ss.substring(0,8)
val hour=ss.substring(8,10)
val minute=ss.substring(10,12)
val sbs=sss.substring(0,17)
val ssss = ss.substring(8)
val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmss")
val lst1: List[Int] = List(1,2,3,4)
val lst2: List[Double] = List(4.1,3.1,2.1,1.1)
val lz: List[(Int, Double)] = lst1.zip(lst2)
/*val config: Config = ConfigFactory.load()
val configObject: ConfigObject = config.getObject("privinceCode")
import scala.collection.JavaConversions._
val map: Map[String, AnyRef] = configObject.unwrapped().toMap
map.foreach(println)*/


