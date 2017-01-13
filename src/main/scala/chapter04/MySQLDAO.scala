package chapter04

/**
  * Created by Administrator on 2016/8/22 0022.
  */
trait MySQLDAO {
  def delete(id: String): Boolean
  def add(o: Any): Boolean
  def update(o: Any): Int
  def query(id: String):List[Any]
}
