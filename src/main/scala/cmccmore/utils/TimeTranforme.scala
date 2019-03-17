package cmccmore.utils

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by yjl
  * Data: 2019/2/21
  * Time: 15:52
  * 时间格式转换
  * 时间戳转毫秒
  */
object TimeTranforme {
  //线程安全
  /* 懒加载, 用到在加载*/
  lazy val format: FastDateFormat = FastDateFormat.getInstance("yyyyMMddhhmmssSSS")

  def getTime(start: String, end: String) = {
    val time: Long = format.parse(end).getTime - format.parse(start).getTime
    time
  }
}
