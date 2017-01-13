package com.hpa.spark.streaming.inputdstream.kafka;

/**
 * Created by Administrator on 2016/12/29.
 */
public class Demo {
    public static void main(String[] args){
        StringBuffer sb = new StringBuffer("Hello ");
        sb.append("World!");
        System.out.println(sb.toString());

        System.out.println("两个参数");
        find(new Integer(1),new Integer(2));

        System.out.println("两个参数");
        find(new Object[]{new Integer(3),new Integer(4)});


        System.out.println("一个参数");
        find(new Integer(1));



    }

    public static void find(Object... params){
        for(int i = 0;i < params.length;i++){
            System.out.println(params[i].toString());
        }
    }
}
