package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 * @author Administrator
 *
 */
public class DataFrameCreate {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame df = sqlContext.read().json("hdfs://AY131210145836355bbcZ:9000/user/caiyf/sql/students.json");  
		
		//df.show();  
		df.groupBy(df.col("age")).count().show();
	}
	
}
