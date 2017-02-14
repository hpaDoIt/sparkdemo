package chapter08

/**
  * Created by Administrator on 2017/1/24.
  */
trait App extends DelayedInt{
  var x: Option[Function0[Unit]] = None

  override def delayedInt(cons: => Unit): Unit = {
    x = Some(() => cons)
  }

  def main(args: Array[String]): Unit = x.foreach(_ ())
}
