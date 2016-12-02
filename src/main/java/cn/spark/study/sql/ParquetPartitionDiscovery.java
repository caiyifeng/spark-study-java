package cn.spark.study.sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ParquetPartitionDiscovery {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
						 .setAppName("ParquetLoadData");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame userDF = sqlContext.read().parquet("hdfs://AY131210145836355bbcZ:9000/user/gender=male/country=US/user.parquet");
		userDF.printSchema();
		userDF.show();
	}

}
