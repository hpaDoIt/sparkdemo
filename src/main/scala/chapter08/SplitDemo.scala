package chapter08

import org.apache.commons.lang.StringUtils


/**
  * Created by Administrator on 2017/2/14.
  */
object SplitDemo {
  def main(args: Array[String]) = {
    val message = "|715|5|00000003b318592a29c7e00d5748f7ed|6|460007242975867|2705248002702|15027014029|18|1464399853343|1464399853428|0|65535|65535|255|255|255|255|47025192|65535|255|4294967295|804|148|3770622043|4294967295|169869021|4294967295|4294967295|4294967295|4294967295|4294967295|4294967295|4294967295|1682962455|4294967295|4294967295|4294967295|1682957076|36412|36422|29081|079103618|65535|4294967295|cmnet.mnc000.mcc460.gprs|1|5|1|6|1|65535|65535|1421328573|2477830215|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|13997508949|460007242975867|353282070294225|35328207|715|1464399853|1464399853|65535|18446744073709551615|4294967295|255|255|343|428";

    val array = StringUtils.splitByWholeSeparatorPreserveAllTokens(message, "|")
    println(s"array$array")
  }
}
