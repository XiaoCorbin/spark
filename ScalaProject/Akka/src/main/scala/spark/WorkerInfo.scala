package spark

//封装worker信息
class WorkerInfo(val workerId:String,val memory:Int,val cores:Int) {

  //定义一个变量用于存放worker上一次心跳时间
  var lastHeartBeatTime:Long=_

  override def toString: String = {
    s"workerId:$workerId , memory:$memory , cores:$cores"
  }
}
