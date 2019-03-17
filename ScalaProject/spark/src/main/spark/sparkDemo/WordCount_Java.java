package sparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class WordCount_Java {
    public static void main(String[] args) {
        //todo:1、构建 sparkconf,设置配置信息
        SparkConf sparkConf = new SparkConf().setAppName("WordCount_Java").setMaster("local[2]");
        //todo:2、构建java 版的 sparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //日志级别
        jsc.setLogLevel("WARN");

        //todo:3、读取数据文件
        JavaRDD<String> file = jsc.textFile("F:\\8.Hadoop\\wordcount\\input\\1.txt");
        //todo:4、对每一行单词进行切分
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                //返回类型Iterator

                return Arrays.asList(words).iterator();
            }
        });
        //todo:5、给每个单词计为 1
        // Spark 为包含键值对类型的 RDD 提供了一些专有的操作。这些 RDD 被称为PairRDD。
        // mapToPair 函数会对一个 RDD 中的每个元素调用 f 函数，其中原来 RDD 中的每一个元素都是 T 类型的，
        // 调用 f 函数后会进行一定的操作把每个元素都转换成一个<K2,V2>类型的对象,其中 Tuple2 为多元组
        JavaPairRDD<String, Integer> wordTuple2 = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                //每个单词记为1:Tuple2(word,1)
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //todo:6、相同单词出现的次数累加
        JavaPairRDD<String, Integer> result = wordTuple2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                //返回单词累加个数integer+integer2->Integer
                return integer + integer2;
            }
        });
        //todo:7、反转顺序(单词.个数)-->(个数,单词)
        JavaPairRDD<Integer, String> IResult = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        });
        //todo:8、把每个单词出现的次数作为 key，进行排序，并且在通过 mapToPair进行反转顺序后输出
        JavaPairRDD<String, Integer> sortResult = IResult.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
         //或者使用 tuple.swap() 实现位置互换，生成新的 tuple;
         @Override
         public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
             return new Tuple2<String, Integer>(tuple._2, tuple._1);
         }
     });
        //收集结果
        List<Tuple2<String, Integer>> finalResult = sortResult.collect();
        //循环打印
        for (Tuple2<String, Integer> tuple2 : finalResult) {
            System.out.println(tuple2._1 + "出现的次数为:" + tuple2._2);
        }

    }
}
