package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ActionOperation {
	public static void main(String[] args) {
		//reduce();
		//collect();
		//count();
		//take();
		//saveAsText();
		countByKey();
	}
	
	private static void reduce() {
	    SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
	    
	    JavaRDD<Integer> numbers = sc.parallelize(numberList);
	    Integer totalNum = numbers.reduce(new Function2<Integer,Integer,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1 + v2;
			}
	    	
	    });
	    
	    System.out.println("totalNum==" + totalNum);
	    sc.close();
	}
	
	private static void collect() {
	    SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
	    
	    JavaRDD<Integer> numbers = sc.parallelize(numberList);	
	    
	    //使用Map操作将集合中数据都乘以 2
	    JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer,Integer>(){

			@Override
			public Integer call(Integer v1) throws Exception {
				
				return v1 * 2;
			}
	    	
	    });
	    
	    //不用foreach action 操作，在远程集群上操作RDD数据
	    //使用collect操作，将分布在远程集群上的doubleNumbers RDD拉取到本地
	    List<Integer> doubleNumberList =  doubleNumbers.collect();
	    for(Integer num: doubleNumberList) {
	    	System.out.println("num : " + num);
	    }
	    sc.close();
	}

	
	private static void count() {
	    SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
	    
	    JavaRDD<Integer> numbers = sc.parallelize(numberList);
	    long count = numbers.count();
	    
	    System.out.println("totalNum==" + count);
	    sc.close();
	}	
	
	private static void take() {
	    SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
	    
	    JavaRDD<Integer> numbers = sc.parallelize(numberList);
	    List<Integer> takedNum =  numbers.take(3);
	    
	    for(Integer num: takedNum) {
	    	System.out.println("num : " + num);
	    }
	    sc.close();
	}	
	
	private static void saveAsText() {
	    SparkConf conf = new SparkConf().setAppName("saveAsText");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
	    
	    JavaRDD<Integer> numbers = sc.parallelize(numberList);	
	    
	    //使用Map操作将集合中数据都乘以 2
	    JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer,Integer>(){

			@Override
			public Integer call(Integer v1) throws Exception {
				
				return v1 * 2;
			}
	    	
	    });
	    
	    //直接将RDD中的数据，保存在文件中
	    doubleNumbers.saveAsTextFile("hdfs://AY131210145836355bbcZ:9000/user/caiyf/output");
	    sc.close();
	}	
	

	
	@SuppressWarnings("unused")
	private static void countByKey() {
		SparkConf conf = new SparkConf()
        .setAppName("countByKey")
        .setMaster("local");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);	
		@SuppressWarnings("unchecked")
		List<Tuple2<String,String>> studentsList = Arrays.asList(
										new Tuple2<String,String>("class1","leo"),
										new Tuple2<String,String>("class2","jack"),
										new Tuple2<String,String>("class1","marry"),
										new Tuple2<String,String>("class2","tom"),
										new Tuple2<String,String>("class2","david"));
		//并行化集合
		JavaPairRDD<String,String> students = sc.parallelizePairs(studentsList);
		
		Map<String, Object> studentCounts = students.countByKey();
		
		for (Map.Entry<String, Object> studentCount :studentCounts.entrySet()) {
			System.out.println(studentCount.getKey() + " : " + studentCount.getValue());
		}
		sc.close();
	}	
}
