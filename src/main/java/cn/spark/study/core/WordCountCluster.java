package cn.spark.study.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * 将java开发的wordcount程序部署到spark集群上运行
 * @author caiyifeng
 *
 */
public class WordCountCluster {
	public static void main(String[] args) {
		//如果要在Spark集群上运行,只要修改两个地方即可
		//第一:将SparkConf的setMaster方法删除，默认会自动去链接
		//我们针对的不是本地文件,修改为读取hadoop hdfs上的存储的数据文件
		
		
		SparkConf conf = new SparkConf().setAppName("WordCountCluster");

		
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		JavaRDD<String> lines = sc.textFile("hdfs://AY131210145836355bbcZ:9000/user/caiyf/wordcount/spark.txt");

		JavaRDD<String> words =  lines.flatMap(new FlatMapFunction<String,String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				
				// TODO Auto-generated method stub
				return Arrays.asList(line.split(" "));
			}
			
		});

		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(
				new PairFunction<String,String,Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String word)
							throws Exception {
						return new Tuple2<String, Integer>(word,1);
					}
					
				}
		);

		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer,Integer,Integer> () {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						
						return v1 + v2;
					}
			
		});
		
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + "  appeared " + wordCount._2 + " times.");
				
			}
		});
		
		sc.close();
	}
}
