import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * * todo:sparksql写入数据到mysql中
  */
//todo:创建样例类Student
 case class Student(id:Int,name:String,age:Int)
object SparkSqlToMysql {
  def main(args: Array[String]): Unit = {
    //todo:1、创建sparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSqlToMysql")
      .master("local[2]")
      .getOrCreate()
    //todo:2、读取数据
    val data: RDD[String] = spark.sparkContext.textFile(args(0))
    //todo:3、切分每一行,
    val arrRDD: RDD[Array[String]] = data.map(_.split(","))
    //todo:4、RDD关联Student
    val studentRDD: RDD[Student] = arrRDD.map(x=>Student(x(0).toInt,x(1),x(2).toInt))
    //todo:导入隐式转换
    import spark.implicits._
    //todo:5、将RDD转换成DataFrame
    val studentDF: DataFrame = studentRDD.toDF()
    //todo:6、将DataFrame注册成表
    studentDF.createOrReplaceTempView("student")
    //todo:7、操作student表 ,按照年龄进行降序排列
    val resultDF: DataFrame = spark.sql("select * from student order by age desc")
    //todo:8、把结果保存在mysql表中
    //todo:创建Properties对象，配置连接mysql的用户名和密码
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","abc")
    resultDF.write.jdbc("jdbc:mysql://localhost:3306/spark","student",prop)
    //todo:写入mysql时，可以配置插入mode，overwrite覆盖，append追加，ignore忽略，error默认表存在报错
    //resultDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.200.150:3306/spark","student",prop)
     spark.stop()

  }
}
