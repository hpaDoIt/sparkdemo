package chapter04;

/**
 * Created by Administrator on 2016/8/22 0022.
 */
public class ScalaInJava {
    public static void main(String[] args){
        Person person = new Person("摇摆少年梦", 27);
        System.out.println("name="+ person.name()+ " age=" + person.age());

        //伴生对象的方法当做静态方法来使用
        System.out.println(Person.getIdentityNo());
    }
}
