import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

//todo:利用sparkStreaming开窗函数统计单位时间内热门词汇----出现频率比较高的词汇

object SparkStreamingSocketWindowHotWords {
  //定义一个方法
  //currentValues:他表示在当前批次每个单词出现的所有的1   (hadoop,1) (hadoop,1)(hadoop,1)
  //historyValues:他表示在之前所有批次中每个单词出现的总次数   (hadoop,100)
  def updateFunc(currentValues:Seq[Int], historyValues:Option[Int]):Option[Int] = {
    val newValue: Int = currentValues.sum+historyValues.getOrElse(0)
    Some(newValue)
  }

  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketWindowHotWords").setMaster("local[2]")
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建streamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置checkpoint目录
    ssc.checkpoint("./ck")
    //4、接受socket数据
    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("node-1",9999)
    //5、切分每一行
    val words: DStream[String] = stream.flatMap(_.split(" "))
    //6、把每一个单词计为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //7、相同单词出现的次数累加
    //reduceByKeyAndWindow该方法需要三个参数
    //reduceFunc：需要一个函数
    //windowDuration:表示窗口的长度
    //slideDuration:表示窗口滑动时间间隔，即每隔多久计算一次
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(5))

    //8、按照单词出现的次数降序排列
    val sortedDstream: DStream[(String, Int)] = result.transform(rdd => {
      //将rdd中数据按照单词出现的次数降序排列
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      //取出前3位
      val sortHotWords: Array[(String, Int)] = sortedRDD.take(3)
      //打印前3位结果数据
      sortHotWords.foreach(x => println(x))
      sortedRDD
    })


    //9、打印排序后的结果数据
    sortedDstream.print()

    //10、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
