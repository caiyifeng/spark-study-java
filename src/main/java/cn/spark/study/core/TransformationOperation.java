package cn.spark.study.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationOperation {
	public static void main(String[] args) {
		//map();
		//filter();
		//flatMap();
		//groupByKey();
		//reduceByKey();
		//sortByKey();
		join();
		//coGroup();
	}
	
	/*
	 *	map算子案例 : 将集体中的每一个元素乘以　２	
	 */
	private static void map(){
		SparkConf conf = new SparkConf()
        .setAppName("map")
        .setMaster("local");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		
		//并行化集合,创建初始RDD
		//map 算子,是对任何类型的RDD，都可以调用
		//在java中,map算子接收的参数是Function对象
		//创建 Function对象，一定会让你设置第二个泛型参数,这个泛型参数，就是返回的新元素的类型
		//call 方法返回的类型，必须与第二个泛型类型同步
		JavaRDD<Integer> numberRdd = sc.parallelize(numbers);
		JavaRDD<Integer> multiNumberRdd = numberRdd.map(new Function<Integer,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				return v1 * 2;
			}
			
		});
		
		multiNumberRdd.foreach(new VoidFunction<Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer v1) throws Exception {
				
				System.out.println("the new num is " + v1);
				
			}
			
		});
		sc.close();
	}
	
	/*
	 * filter算子案例,过滤集合中的偶数
	 */
	
	private static void filter() {
		SparkConf conf = new SparkConf()
        .setAppName("filter")
        .setMaster("local");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		//filter 算子，传入的也是Function，但call方法返回的方法类型是Boolean
		//每一个初始RDD元素,都会传入call()方法,此时你可以执行各种自定义的逻辑运算来判断该元素是否是你想要的
		//如果想在新的RDD中保留该元素，那么返回TRUE，否则返回FALSE
		JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer,Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				return v1 % 2 == 0;
			}
			
		});
		
		
		evenNumberRDD.foreach(new VoidFunction<Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer v1) throws Exception {
				
				System.out.println("the new num is " + v1);
				
			}
			
		});	
		
		sc.close();
	}
	
	
	/*
	 * flagMap算子案例,过滤集合中的偶数
	 */
	
	private static void flatMap() {
		SparkConf conf = new SparkConf()
        .setAppName("flatMap")
        .setMaster("local");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> lineList = Arrays.asList("hello you","hello me","hello word");
		
		JavaRDD<String> lines = sc.parallelize(lineList);
		
		//flagMap 算子，传入的也是FlatMapFunction
		//我们需要定义该函数的第二个泛型类型,即代表了返回的新元素的类型
		//call方法，返回的类型不是U，而是Iterable<U>,这里的U就是第二个泛型类型
		//返回的是多个元素，放在Iterable 集合中,可以使用ArrayList等集合
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				
				return  Arrays.asList(t.split(" "));
			}
			
		}) ;
				
		words.foreach(new VoidFunction<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String v1) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(v1);
			}
			
		});
		
		sc.close();
	}	
	
	//groupByKey 案例,按照班级对成绩进行分组
	private static void groupByKey() {
		SparkConf conf = new SparkConf()
        .setAppName("groupByKey")
        .setMaster("local");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		@SuppressWarnings("unchecked")
		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
										new Tuple2<String,Integer>("class1",80),
										new Tuple2<String,Integer>("class2",80),
										new Tuple2<String,Integer>("class1",90),
										new Tuple2<String,Integer>("class2",75));
		//并行化集合
		JavaPairRDD<String,Integer> scores = sc.parallelizePairs(scoreList);
		
		//针对scores RDD，执行groupByKey算子,对每个班级成绩进行分组
		JavaPairRDD<String,Iterable<Integer>> groupedScores = scores.groupByKey();
		
		groupedScores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				System.out.println("class: " + t._1);
				
				Iterator<Integer> ite = t._2.iterator();
				while (ite.hasNext()) {
					System.out.println(ite.next());
				}
				
			}
			
		});
		
		sc.close();
	}
	
	//reduceByKey 案例,按照班级对成绩进行分组
	private static void reduceByKey() {
		SparkConf conf = new SparkConf()
        .setAppName("groupByKey")
        .setMaster("local");	
		
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		@SuppressWarnings("unchecked")
		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
										new Tuple2<String,Integer>("class1",80),
										new Tuple2<String,Integer>("class2",80),
										new Tuple2<String,Integer>("class1",90),
										new Tuple2<String,Integer>("class2",75));
		//并行化集合
		JavaPairRDD<String,Integer> scores = sc.parallelizePairs(scoreList);
		
		//针对scores RDD，执行reduceByKey算子
		//第一个泛型
		JavaPairRDD<String,Integer> reducedScores = scores.reduceByKey(new Function2<Integer,Integer,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1 + v2;
			}
			
		});
		
	
		reducedScores.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t._1 + ":  " + t._2);
			}
			
		});
		
		sc.close();
	}
	
	//sortBykey 案例：按照学生分数进行排序
	
	private static void sortByKey() {
		SparkConf conf = new SparkConf()
        .setAppName("sortByKey")
        .setMaster("local");	
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer,String>> scoreList = Arrays.asList(
				new Tuple2<Integer,String>(80,"Tom"),
				new Tuple2<Integer,String>(86,"Jack"),
				new Tuple2<Integer,String>(90,"Leo"),
				new Tuple2<Integer,String>(75,"King"));
		
		JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<Integer, String> sortedScore = scores.sortByKey(false);
		
		sortedScore.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t._1+ ": " + t._2);
			}
			
		});
		
		sc.close();
	}
	
	
	//join和cogroup 案例：打印学生成绩
	
	private static void join() {
		SparkConf conf = new SparkConf()
        .setAppName("joinAndCogroup")
        .setMaster("local");	
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer,String>> studentList = Arrays.asList(
					new Tuple2<Integer,String>(1,"leo"),
					new Tuple2<Integer,String>(2,"jack"),
					new Tuple2<Integer,String>(3,"tom"));
		
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
					new Tuple2<Integer,Integer>(1,100),
					new Tuple2<Integer,Integer>(2,90),
					new Tuple2<Integer,Integer>(3,60),
					new Tuple2<Integer,Integer>(1,98),
					new Tuple2<Integer,Integer>(2,86),
					new Tuple2<Integer,Integer>(3,75));
		
		JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
		
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
		//使用join关联两个RDD
		
		JavaPairRDD<Integer, Tuple2<String,Integer>> studentScores = students.join(scores);
		
		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
					throws Exception {
				// TODO Auto-generated method stub
				System.out.println("student id : " + t._1);
				System.out.println("student name: " + t._2._1);
				System.out.println("student score: " + t._2._2);
			}
			
		});
		sc.close();
	}	
	
	//coGroup 与 join不同,相当于一个key join上的所有value 都放到一个Iterable上面去了
	private static void coGroup() {
		SparkConf conf = new SparkConf()
        .setAppName("joinAndCogroup")
        .setMaster("local");	
		JavaSparkContext sc = new JavaSparkContext(conf);	
		
		List<Tuple2<Integer,String>> studentList = Arrays.asList(
					new Tuple2<Integer,String>(1,"leo"),
					new Tuple2<Integer,String>(2,"jack"),
					new Tuple2<Integer,String>(3,"tom"));
		
		List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
					new Tuple2<Integer,Integer>(1,100),
					new Tuple2<Integer,Integer>(2,90),
					new Tuple2<Integer,Integer>(3,60),
					new Tuple2<Integer,Integer>(1,94),
					new Tuple2<Integer,Integer>(2,89),
					new Tuple2<Integer,Integer>(3,78));
		
		JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
		
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
		//使用coGroup关联两个RDD
		
		JavaPairRDD<Integer, Tuple2<Iterable<String>,Iterable<Integer>>> studentScores = students.cogroup(scores);   
		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
					throws Exception {
					
				System.out.println("student id : " + t._1);
				System.out.println("student name: " + t._2._1);
				System.out.println("student score: " + t._2._2);				
				
			}
			
		});
		sc.close();
	}	
}
