package chapter04.javaandscalageneric

import java.util.Comparator

/**
  * Scala类型参数与JAVA泛型互操作
  * Created by hpa on 2016/8/23 0023.
  */

case class Person(val name: String,val age: Int)

class PersonComparator extends Comparator[Person]{
  override def compare(o1: Person, o2: Person): Int = if(o1.age > o2.age) 1 else -1
}
object ScalaUseJavaComparator extends App{
  val p1 = Person("张三",23)
  val p2 = Person("马子",25)

  val personComparator = new PersonComparator()
  if(personComparator.compare(p1,p2)>0)
    println(p1)
  else println(p2)

}
