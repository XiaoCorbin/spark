//DataFrame读取多种数据源,通过Dataframe进行数据处理并且写入指定存储位置
//本例子:利用DataFrame处理数据的格式转换
object scala00 {
  def main(args: Array[String]): Unit = {
  
/*      读取json或者parquet文件创建一个DataFrame

      DataFrame存储到某一个路径下
      ， 默认存储格式是parquet

      SaveMode.Overwrite
      ： 重写
*/

    //to:设置SparkContext
    SparkConf conf = new SparkConf()
      .setAppName("SaveModeTest")
      .setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    //todo:去hdfs读取文件，返回DataFrame格式
    DataFrame peopleDF = sqlContext.read().json("hdfs://hadoop1:9000/input/people.json");

    //todo:存储保存到hdfs上，以parquet格式
    peopleDF.write().mode(SaveMode.Overwrite).save("hdfs://hadoop1:9000/output/namesAndFavColors_scala");

    //todo:验证是否parquet，就是以该格式读取显示
    sqlContext.read().format("parquet").load("hdfs://hadoop1:9000/output/namesAndFavColors_scala").show();

    // sqlContext.read().parquet("hdfs://hadoop1:9000/output/namesAndFavColors_scala").show();

  }
}
