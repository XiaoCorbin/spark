import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * RDD转换成DataFrame:通过指定schema构建DataFrame
  */
object SparkSqlSchema {
//  当case class不能提前定义好时
def main(args: Array[String]): Unit = {
  //todo:1、创建SparkSession,指定appName和master
  val spark: SparkSession = SparkSession.builder()
    .appName("SparkSqlSchema")
    .master("local[2]")
    .getOrCreate()
  //todo:2、获取sparkContext对象
  val sc: SparkContext = spark.sparkContext
  //todo:3、加载数据
  val dataRDD: RDD[String] = sc.textFile("G:\\person.txt")
  //todo:4、切分每一行
   val dataArrayRDD: RDD[Array[String]] = dataRDD.map(_.split(" "))
  //todo:5、加载数据到Row对象中
   val personRDD: RDD[Row] = dataArrayRDD.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
  //todo:6、创建schema
   val schema:StructType= StructType(Seq(
                 StructField("id", IntegerType, false),
                 StructField("name", StringType, false),
                 StructField("age", IntegerType, false)
   ))
  //todo:7、利用personRDD与schema创建DataFrame
   val personDF: DataFrame = spark.createDataFrame(personRDD,schema)
  //todo:8、DSL操作显示DataFrame的数据结果
   personDF.show()
  // todo:9、将DataFrame注册成表
   personDF.createOrReplaceTempView("t_person")
  // todo:10、sql语句操作
   spark.sql("select * from t_person").show()
   spark.sql("select count(*) from t_person").show()
   sc.stop()
   spark.stop()

}
}
