package cn.spark.study.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
public class ParquetMergeSchema {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
		 .setAppName("ParquetMergeSchema");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		@SuppressWarnings("unchecked")
		List<Tuple2<String,Integer>> studentsWithNameAge = Arrays.asList(
				new Tuple2<String,Integer>("leo",23),
				new Tuple2<String,Integer>("jack",28));
		
		JavaRDD<Tuple2<String,Integer>> students = sc.parallelize(studentsWithNameAge,2);
		
		JavaRDD<Row> studentRow = students.map(new Function<Tuple2<String,Integer>,Row> () {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Integer> student) throws Exception {
				
				return RowFactory.create(student._1,student._2);
				
			}
			
		});
		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);		
		
		DataFrame studentDF = sqlContext.createDataFrame(studentRow, structType);
		studentDF.printSchema();
		studentDF.show();
		studentDF.save("hdfs://AY131210145836355bbcZ:9000/user/caiyf/students", "parquet", SaveMode.Append);
		
		List<Tuple2<String,String>> studentsWithNameGrade = Arrays.asList(
				new Tuple2<String,String>("marry","A"),
				new Tuple2<String,String>("tom","B"));		
		
		
		JavaRDD<Tuple2<String,String>> studentGrades = sc.parallelize(studentsWithNameGrade,2);
		
		JavaRDD<Row> studentGradeRow = studentGrades.map(new Function<Tuple2<String,String>,Row> () {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, String> student) throws Exception {
				
				return RowFactory.create(student._1,student._2);
				
			}
			
		});	
		
		List<StructField> structFields_Grade = new ArrayList<StructField>();
		structFields_Grade.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields_Grade.add(DataTypes.createStructField("grade", DataTypes.StringType, true));
		StructType gradeStructType = DataTypes.createStructType(structFields_Grade);		
		DataFrame studentGradeDF = sqlContext.createDataFrame(studentGradeRow, gradeStructType);
		
		studentGradeDF.printSchema();
		studentGradeDF.show();
		studentGradeDF.save("hdfs://AY131210145836355bbcZ:9000/user/caiyf/students", "parquet", SaveMode.Append);
		DataFrame allDF = sqlContext.read().option("mergeSchema", "true").parquet("hdfs://AY131210145836355bbcZ:9000/user/caiyf/students");
		allDF.printSchema();
		allDF.show();
//	    val students = sqlContext.read.option("mergeSchema", "true")
//	            .parquet("hdfs://spark1:9000/spark-study/students")
//	        students.printSchema()
//	        students.show()  		
		
	}

}
