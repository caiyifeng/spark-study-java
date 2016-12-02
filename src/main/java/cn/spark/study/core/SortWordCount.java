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

public class SortWordCount {

	public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);			
	    JavaRDD<String> lines = sc.textFile("D://temp//spark.txt");
		JavaRDD<String> words =  lines.flatMap(new FlatMapFunction<String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

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
		
		JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(
					new PairFunction<Tuple2<String,Integer>,Integer,String>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<Integer, String> call(
								Tuple2<String, Integer> t) throws Exception {
							
							return new Tuple2<Integer, String>(t._2,t._1);
						}
			
		});
		
		JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);
		
		JavaPairRDD<String,Integer> sortedWordCounts = sortedCountWords.mapToPair(
					new PairFunction<Tuple2<Integer,String>,String,Integer>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Integer> call(
								Tuple2<Integer, String> t) throws Exception {
							
							return new Tuple2<String,Integer>(t._2,t._1);
						}
						
		}); 
		
		
		sortedWordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> v1) throws Exception {
				
				System.out.println(v1._1 + ": " + v1._2);
				
			}
			
		});
	    sc.close();
	}

}
