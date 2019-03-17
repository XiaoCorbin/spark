package ip

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * ip地址查询
  **/
object IPLocaltion {

  //将ip地址转换为long
  def ipToLong(ip: String): Long = {
    //ip:Long,固定算法
    //todo：切分ip地址。
    val ipArray: Array[String] = ip.split("\\.")
    var ipNum = 0L
    for (i <- ipArray) {
      ipNum = i.toLong | ipNum << 8L
    }
    //方法结果:返回ipNum
    ipNum
  }

  //二分法查找ip范围
  def binarySearch(ipNum: Long, value: Array[(String, String, String, String, String)]): Int = {
    //todo:口诀：上下循环寻上下，左移右移寻中间
    // 开始下标
    var start = 0
    // 结束下标
    var end = value.length - 1

    while (start <= end) {
      val middle = (start + end) / 2
      if (ipNum >= value(middle)._1.toLong && ipNum <= value(middle)._2.toLong) {
        return middle
      }
      if (ipNum > value(middle)._2.toLong) {
        start = middle
      }
      if (ipNum < value(middle)._1.toLong) {
        end = middle
      }
    }
    -1

  }

  //todo:将结果保存到mysql表中
  def data2Mysql(iter:Iterator[(String,String, Int)])= {
    //todo:创建数据库连接Connection
    var conn: Connection = null
    //todo:创建PreparedStatement对象
    var ps: PreparedStatement = null
    //todo:采用拼占位符问号的方式写sql语句
    var sql = "insert into iplocaltion(longitude,latitude,total_count) values(?,?,?)"
    //todo:获取数据连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "abc")
    //todo:  选中想被try/catch包围的语句 ctrl+alt+t 快捷键选中try/catch/finally
    try {
      iter.foreach(line => {
        //todo:预编译sql语句
        ps = conn.prepareStatement(sql)
        //todo:对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
        ps.setString(1, line._1)
        ps.setString(2, line._2)
        ps.setLong(3, line._3)
        //todo:执行
        ps.execute()
      })
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    //1.获取ip数据
    //todo:创建sparkconf 设置参数
    val sparkConf: SparkConf = new SparkConf().setAppName("IPLocaltion_Test").setMaster("local[2]")
    //todo：创建SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)

    //todo：读取基站数据
    val data: RDD[String] = sc.textFile("G:\\ip.txt")
    //todo:对基站数据进行切分 ，获取需要的字段 （ipStart,ipEnd,城市位置，经度，纬度）
    val jizhanRDD: RDD[(String, String, String, String, String)] = data.map(_.split("\\|")).map(x => (
      x(2),
      x(3),
      x(4) + "-" + x(5) + "-" + x(6) + "-" + x(7) + "-" + x(8),
      x(13),
      x(14)))
    //todo:获取基站RDD的数据
    val jizhanData: Array[(String, String, String, String, String)] = jizhanRDD.collect()

    //2.将ip数据广播给所有的task
    //todo:广播变量，sc.broadcast(jizhanData):一个只读的数据区，所有的task都能读到的地方
    val broadCast: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(jizhanData)


    //3.将从广播获取到的ip查询ip-address目录中对应的address
    //todo:读取目标数据(ip目录表:所有ip对应的位置),获取数据中的ip地址字段
    val ipData: RDD[String] = sc.textFile("G:\\20090121000132.394251.http.format").map(_.split("\\|")).map(x => x(1))
    //todo:把IP地址转化为long类型，然后通过二分法去基站数据中查找，找到的维度做wordCount
    //mapPartitions对每个分区进行操作,提高效率

    val result = ipData.mapPartitions(iter => {
      //获取广播的值
      val value: Array[(String, String, String, String, String)] = broadCast.value
      //遍历迭代器获取每一个ip地址
      iter.map(ip => {
        //将ip转化为数字long
        val ipNum: Long = ipToLong(ip)
        //拿这个数字long去基站数据中通过二分法查找，返回ip在valueArr中的下标
        val index: Int = binarySearch(ipNum, value)
        //根据下标获取对一个的经纬度
        val tuple = value(index)
        //返回结果 ((经度，维度)，1)
        ((tuple._4, tuple._5), 1)
      })
    })

    //todo:分组聚合
    val resultFinal: RDD[((String, String), Int)] = result.reduceByKey(_ + _)
    //todo:打印输出
    resultFinal.foreach(println)

    //todo:将结果保存到mysql表中
    resultFinal.map(x => (x._1._1, x._1._2, x._2)).foreachPartition(data2Mysql)

    //stop
    sc.stop()

  }
}

