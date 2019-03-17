package rpc

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

//todo:利用akka的actor模型实现2个进程间的通信-----Master端

class Master extends Actor {
  //构造代码块先被执行
  println("master constructor invoked")

  //prestart方法会在构造代码块执行后被调用，并且只被调用一次
  override def preStart(): Unit = {
    println("preStart method invoked")
  }

  //receive方法会在prestart方法执行后被调用，表示不断的接受消息
  override def receive: Receive = {
    case "master-connect-self" => {
      //master-master
      println("master client connected")

    }
      //worker-master
    case "worker-2-master" => {
      println("worker client connected")

      //master发送注册成功信息给worker
      sender ! "worker-2-master success"
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    //env配置
    //    val  host="192.168.46.51"
    //    val port=9999
    val host = args(0)
    val port = args(1)

    val configStr =
      s"""
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = "$host"
        |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    //配置config对象 利用ConfigFactory解析配置文件，获取配置信息
    val conf = ConfigFactory.parseString(configStr)
    // 1、创建ActorSystem,它是整个进程中老大，
    // 它负责创建和监督actor，它是单例对象
    val masterActorSystem = ActorSystem("masterActorSystem", conf)

    // 2、通过ActorSystem来创建master actor
    val masterActor: ActorRef = masterActorSystem.actorOf(Props(new Master), "masterActor")

    // 3、向master actor发送消息
//    masterActor ! "master-connect-self"
  }
}
