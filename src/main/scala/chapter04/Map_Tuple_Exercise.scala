package chapter04

/**
  * Created by hpa on 2016/8/17 0017.
  */
object Map_Tuple_Exercise {
  def main(args: Array[String]){
    //1创建静态映射
    val scores1 = Map("Alice" -> 10,"Bob" -> 3,"Cindy" -> 8)
    val scores2 = Map(("Alice" , 10),("Bob" , 3),("Cindy" , 8))

    //2创建可变映射
    val scores3 = scala.collection.mutable.Map("Alice" -> 10,"Bob" -> 3,"Cindy" -> 8)
    val scores4 = scala.collection.mutable.Map(("Alice" , 10),("Bob" , 3),("Cindy" , 8))

    //3.获得映射中的值
    //注意：如果映射中并不包含请求中使用的键，则会抛出异常
    val bobsScore1 = scores1("Bob")

    //4.要检查映射中是否有某个指定的键
    val bobsScore2 = if(scores1.contains("Bob"))scores1("Bob") else 0
    val bobsScore3 = scores1.getOrElse("Bob",0)

    //5.1更新可变映射中的值
    scores3("Bob") = 30
    scores3("Fred") = 7

    //5.2可以用+=向可变映射中添加多个关系
    scores3 += ("Bob" -> 10,"Fred" -> 7)

    //5.3要移除某个键和对应的值，使用-=操作符
    scores3 -= "Alice"

    //5.4不能更新一个不可变的映射，但是可以做一些同样有用的操作——获取一个包含所需要的更新的新映射
    val newScores = scores1 + ("Bob" -> 10,"Fred" -> 7)

    //5.5也可以更新var变量
    var scores = scores1 + ("Bob" -> 10,"Fred" -> 7)

    //5.6要从不可变映射中移除某个键，可以用-操作符来获取一个新的去掉该键的映射
    scores = scores1 - "Alice"

    //说明：这样不停地创建新映射效率很低，不过事实并非如此。老的和新的映射共享大部分结构

    //6.遍历映射中所有的键/值对偶，语法：for((k,v) <- 映射) 处理k和v
    for((k,v) <- scores1)
      println("k: " + k + ",v: " + v)

    for(k <- scores1.keySet)
      println("k: " + k)

    for(v <- scores1.values)
      println("v: " + v)

    //7.要反转一个映射——即交换键和值的位置，语法：for((k,v) <- 映射) yield (v,k)
    val scores5 = for((k,v) <- scores1) yield (v,k)

    for((k,v) <- scores5)println("k: " + k + ",v: " + v)

    //8.已排序映射，在操作映射时，需要选定一个实现——一个哈希表或者一个平衡树。默认情况下，
    //Scala给的是哈希表
    //8.1要得到一个不可变的树形映射而不是哈希映射的话
    val scores8 = scala.collection.immutable.SortedMap("Alice" -> 10,"Fred" -> 7,"Bob" -> 3,"Cindy" -> 8)
    for((k,v) <- scores8) println("k: " + k + ",v: " + v)

    //9.与Java的互操作


  }
}
