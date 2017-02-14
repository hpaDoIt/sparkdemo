package chapter08

/**
  * Created by Administrator on 2017/1/24.
  */
object DelayedIntTest {
  def main(args: Array[String]): Unit ={
    val x = new App {println("Now I'm initialized")}
    x.main(Array())
  }
}
