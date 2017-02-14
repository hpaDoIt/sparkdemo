package chapter08

/**
  * Created by Administrator on 2017/1/24.
  */
trait DelayedInt {
  def delayedInt(x: => Unit):Unit
}