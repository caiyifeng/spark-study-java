package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ParallelizeCollection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ParallelizeCollection")
						 .setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		int sum = numberRDD.reduce(new Function2<Integer,Integer,Integer> () {

			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				// TODO Auto-generated method stub
				return num1 + num2;
			}
				
		});
		
		System.out.println("sum = " + sum);
		sc.close();
	}
}
