package spark

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._


//todo:利用akka实现简易版的spark通信框架-----Worker端
class Worker(val memory: Int, val cores: Int, val masterHost: String, val masterPort: String) extends Actor {
  println("Worker constructor invoked")

  //workerId
  private val workerId = UUID.randomUUID().toString

  //定义发送心跳的时间间隔
  val SEND_HEART_HEAT_INTERVAL = 10000 //10秒

  //定义全局变量
  var master: ActorSelection = _

  //prestart方法会在构造代码块之后被调用，并且只会被调用一次
  override def preStart(): Unit = {
    println("preStart method invoked")
    //获取master actor的引用
    //ActorContext全局变量:可以通过在已经存在的actors中,寻找目标actor
    //调用对应actorSelection方法，
    // 方法需要一个path路径：
    // 1、通信协议、
    // 2、master的IP地址、
    // 3、master的端口
    // 4、创建master actor老大
    // 5、actor层级

    master = context.actorSelection("akka.tcp://masterActorSystem@192.168.46.43:8888/user/masterActor")

    //向master发送消息
    //    master ! "worker-2-master"
    //向master发送消息:信息封装在样例类中，主要包含：workerId,memory,cores
    master ! RegisterMessage(workerId, memory, cores)
  }


  //receive方法会在prestart方法执行后被调用，不断的接受消息
  override def receive: Receive = {
    //worker接受master的反馈信息
    case RegisteredMessage(message) => {
      //master-worker
      println(message)

      //向master定期的发送心跳
      //worker先自己给自己发送心跳 //需要手动导入隐式转换
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, SEND_HEART_HEAT_INTERVAL millis, self, HeartBeat)

    }

    //worker接受自己心跳:证明心跳正常,才开始向master发送心跳
    case HeartBeat => {
      //这个时候才是真正向master发送心跳
      master ! SendHeartBeat(workerId)
      println(workerId)
    }


  }
}


object Worker {
  def main(args: Array[String]): Unit = {


    //env配置
    val host = args(0)
    val port = args(1)

    //定义worker的内存
    val memory = args(2).toInt
    //定义worker的核数
    val cores = args(3).toInt

    //定义master的ip地址
    val masterHost = args(4)
    //定义master的端口
    val masterPort = args(5)

    //准备配置文件
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
    """.stripMargin

    //通过configFactory来解析配置信息
    val conf = ConfigFactory.parseString(configStr)
    // 1、创建ActorSystem，它是整个进程中的老大，它负责创建和监督actor
    val workerActorSystem = ActorSystem("workerActorSystem", conf)
    // 2、通过actorSystem来创建 worker actor
    val workerActor: ActorRef = workerActorSystem.actorOf(Props(new Worker(memory, cores, masterHost, masterPort)), "workerActor")

    //向worker actor发送消息
    //  workerActor ! "worker-self-connect"
  }
}