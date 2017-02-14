package com.hpa.spark.adapter.main

import akka.actor.Actor

/**
  * Created by hpa on 2017/2/9.
  */
class MyActor extends Actor{
  override def receive: Receive = {
    case input: String => println(input)
    case _ => ()
  }
}
