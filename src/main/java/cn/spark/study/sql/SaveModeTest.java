package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * SaveModel示例
 * @author Administrator
 *
 */
public class SaveModeTest {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()   
				.setMaster("local")
				.setAppName("SaveModeTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame peopleDF = sqlContext.read().format("json")
				.load("D://temp//people.json"); 
		peopleDF.save("D://temp//people_savemode_test", "json", SaveMode.Append);
	
	}
}
