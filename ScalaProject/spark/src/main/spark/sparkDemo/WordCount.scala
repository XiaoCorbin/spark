package sparkDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //设置 spark 的配置文件信息
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")

    //构建 sparkcontext 上下文对象， 它是程序的入口,所有计算的源头
    val sc: SparkContext = new SparkContext(sparkConf)
    //日志级别
    sc.setLogLevel("WARN")

    //读取文件
    val file: RDD[String] = sc.textFile(args(0))
    //对文件中每一行单词进行压平切分
    val words: RDD[String] = file.flatMap(_.split(" "))
    //对每一个单词计数为 1 转化为(单词， 1)
    val wordAndOne: RDD[(String, Int)] = words.map(x=>(x,1))
    //相同的单词进行汇总 前一个下划线表示累加数据， 后一个下划线表示新数据
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序排列
    val sortResult: RDD[(String, Int)] = result.sortBy(_._2,true)
    //收集数据
    val finalResult: Array[(String, Int)] = sortResult.collect()
    //保存数据到 HDFS
    finalResult.foreach(x=>println(x))
//    finalResult.(args(1))
    //stop
    sc.stop()
  }
}
