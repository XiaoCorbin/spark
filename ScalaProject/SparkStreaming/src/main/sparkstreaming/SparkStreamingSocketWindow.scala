import SparkStreamingTCPTotal.updateFunc
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//todo:利用sparkStreaming开窗函数 reducebyKeyAndWindow实现单词计数
object SparkStreamingSocketWindow {

  //定义一个方法
  //currentValues:他表示在当前批次每个单词出现的所有的1   (hadoop,1) (hadoop,1)(hadoop,1)
  //historyValues:他表示在之前所有批次中每个单词出现的总次数   (hadoop,100)
//  def reduceFunc(currentValues:Seq[Int], historyValues:Option[Int]) ={
//    val newValue: Int = currentValues.sum+historyValues.getOrElse(0)
//    Some(newValue)
//  }

  def main(args: Array[String]): Unit = {
    //1.配置sparkConf参数:
    //local[N]的N必须大于1,一个接收,其他处理...
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingTCPTotal").setMaster("local[2]")
    //2.构建sparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    //3.设置日志输出级别
    sc.setLogLevel("WARN")

    //4.构建StreamingContext对象，每个批处理的时间间隔
    val scc: StreamingContext = new StreamingContext(sc,Seconds(5))
    //    requirement failed: The checkpoint directory has not been set
    scc.checkpoint("./ck")
    //5.注册一个监听的IP地址和端口  用来收集数据
    val lines: ReceiverInputDStream[String] = scc.socketTextStream("node-1",9999)

    //6.切分每一行记录
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //7.每个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //8.累计统计单词出现的次数
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(5),Seconds(5))

    result.print()
    //9.开启流式计算
    scc.start()
    //一直会阻塞,等待退出
    scc.awaitTermination()
  }

}
