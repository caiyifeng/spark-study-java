package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastVariable {

	public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);		
	    //使用共享变量，每个节点只拷贝一份factor的副本,否则是每个task都拷贝一份factor的副本
	    //在JAVA中创建共享变量,使用的是SparkContext中的broadcast()方法
	    final int factor = 3;
	    final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
	    List<Integer> numberList = Arrays.asList(1,2,3,4,5);
	    JavaRDD<Integer> numbers = sc.parallelize(numberList);
	    //让集合中的每个数字，都乘以外部定义的factor
	    
	    JavaRDD<Integer> multipleNumbers =  numbers.map(new Function<Integer,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				int factor = factorBroadcast.value();
				return v1 * factor;
			}
	    	
	    });
	    
	    multipleNumbers.foreach(new VoidFunction<Integer> () {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			} 
	    	
	    });
	    
	    sc.close();
	}

}
