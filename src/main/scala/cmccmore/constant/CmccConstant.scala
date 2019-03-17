package cmccmore.constant

/**
  * Created by yjl
  * Data: 2019/2/21
  * Time: 23:23
  * 保存常量
  * 枚举类型
  */
object CmccConstant extends Enumeration {
  val PROJECT = Value("cmcc")
  /*封装两个字段名称*/
  val SERNAME= Value("serviceName")
  val RECHARGENR=Value("reChargeNotifyReq")
  /*时间戳 截取day的起始位置*/
  val DAYSTART=0
  val DAYEND=8


}
