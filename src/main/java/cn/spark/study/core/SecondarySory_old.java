package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/*
 * 二次排序
 * 1. 实现自定义的key，要实现Ordered接口和Serializable接口,在 key中实现自己对多个列的排序算法
 * 2. 将包含文本的RDD，映射成key为自定义(key,value为文本的JavaPairRDD
 * 3. 使用sortByKey算子按照自定义的key尽心排序
 * 4. 再次映射，剔除自定义的key，剩下已经排序好的文本
 * */
public class SecondarySory_old {

	public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("SecondarySory").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);		
	    
	    JavaRDD<String> lines = sc.textFile("D://temp//sort.txt");
	    JavaPairRDD<SecondarySortKey_old, String> pairs = lines.mapToPair(new PairFunction<String,SecondarySortKey_old,String>() {

			@Override
			public Tuple2<SecondarySortKey_old, String> call(String line) throws Exception {
				String[] lineSplited = line.split(" ");
				SecondarySortKey_old key = new SecondarySortKey_old(
						Integer.valueOf(lineSplited[0]),
						Integer.valueOf(lineSplited[1]));
				
				return new Tuple2<SecondarySortKey_old,String>(key,line);
			}
	    	
	    });
	    
	    JavaPairRDD<SecondarySortKey_old, String> sortedPairs = pairs.sortByKey();
	    JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey_old,String>,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<SecondarySortKey_old, String> v1)
					throws Exception {
				
				return v1._2;
			}
	    	
	    });
	    
	    sortedLines.foreach(new VoidFunction<String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
	    	
	    });
	    sc.close();
	}

}
