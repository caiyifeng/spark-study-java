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
 * 本地测试的wordcount程序
 * @author caiyifeng
 *
 */
public class WordCountLocal {
	public static void main(String[] args) {
		//编写spark应用
		//第一步:创建SparkConf对象，设置Spark应用的配置信息
		//使用setMaster(),设置spark应用程序要连接的sark集群的master节点的url
		//但是如果设置为local则代表了，在本地运行
		
		SparkConf conf = new SparkConf()
		                 .setAppName("WordCountLocal")
		                 .setMaster("local");
		
		//第二步:创建JavaSparkContext对象
		//在Spark中,SparkContext是Spark所有功能的一个入口
		//SparkContext的作用:包括初始化一些核心组件,包括
		//调度器(DAGSchedule,TaskSchedule)，还会去Spark Master去注册等等
		//是Spark中最重要的一个对象
		//但是Spark中,编写不同的Spark应用程序,使用的SparkContext是不同的，如果使用Scala,
		//使用的就是原生的SparkContext对象
		//比如Java: 就是JavaSparkContext
		//Spark SQL应用: 就是SQLContext，HiveContext
		//Spark Streaming: 就是 SparkContext
		//以此类推
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//第三步:要针对输入源(hdfs,本地文件等)，创建一个初始的RDD
		//输入源的数据会打散,分配到RDD的每一个partition中,从而形成一个初始的分布式的数据集
		//我们这里呢,因为是本地测试,所以呢,就是针对本地文件
		//SparkContext中,用于根据文件类型的输入源创建RDD的方法,叫做textFile()方法
		//在Java中,创建的普通RDD，都叫JavaRDD
		//在这里,RDD中,有元素的概念,如果是hdfs或者是本地文件,创建的RDD，每一个元素就相当于文件里的一行
		
		JavaRDD<String> lines = sc.textFile("D://temp//spark.txt");
		//第四步: 对初始RDD进行transformation操作
		//
		//transformation操作 通常会创建function,并配合RDD的map,flagMap等算子来执行
		//function: 通常比较简单的话,则创建指定functionde的匿名内部类
		//但是如果function比较复杂,则会单独创建一个类,作为实现这个function接口的类
		//先将每一行拆分成单个的单词
		//FlatMapFunction,有两个泛型参数,分别代表了输入和输出类型
		//flagMap的作用是把RDD的一个元素,拆分成1个或多个元素
		JavaRDD<String> words =  lines.flatMap(new FlatMapFunction<String,String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				
				// TODO Auto-generated method stub
				return Arrays.asList(line.split(" "));
			}
			
		});
		//接着，需要将每一个单词,映射为(单词,1)的格式
		//因为只有这样,后面才能根据单词作为key，来进行每个单词的计数
		//mapToPair ,其实就是将每个元素,映射为一个 (v1,v2)的Tuple2类型元素
		//这个相当于scala中的tuple
		//mapToPair这个算子,要求PairFunction函数配合使用,第一个泛型参数代表输入类型
		//第二，第三个泛型参数，代表输出的Tuple2的第一个值和第二个值
		//JavaPairRDD的两个泛型参数,分别代表了Tuple的第一个值和第二个值
		
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
		//接着，需要以单词作为key，统计每个单词出现的次数
		//这里要使用reduceByKey算子,对每个Key对应的value进行reduce操作
		//
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
		
		//到这为止，我们通过几个Spark算子操作，已经统计出了单词的次数
		//之前我们使用的flagMap,mapToPair,reduceByKey等操作，叫做transformation操作
		//一个Spark应用中,光只有transformation操作，是不行俄，程序不会执行,必须有一种叫做action的操作，
		//来触发程序的执行
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + "  appeared " + wordCount._2 + " times.");
				
			}
		});
		
		sc.close();
	}
}
