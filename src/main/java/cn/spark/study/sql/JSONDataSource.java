package cn.spark.study.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JSONDataSource {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
		.setAppName("DataFrameCreate");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		//针对json文件,创建DataFrame
		DataFrame studentScoresDF = sqlContext.read().json("hdfs://iZ113mzdkz0Z:9000/user/caiyf/students/students_grade.json");
		
		//注册临时表，查询分数大于80分的学生的姓名
		studentScoresDF.registerTempTable("student_scores");
		
		DataFrame goodStudentDF = sqlContext.sql("select name,score from student_scores where score>=80");
		
		List<String> goodStudentName = goodStudentDF.javaRDD().map(new Function<Row,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {

				return row.getString(0);
			}
			
		}).collect();
		
		//创建学生基本信息,并转为DataFrame
		List<String> studentInfos = new ArrayList<String>();
		studentInfos.add("{\"name\":\"Leo\",\"age\":18}");
		studentInfos.add("{\"name\":\"Marry\",\"age\":17}");
		studentInfos.add("{\"name\":\"Jack\",\"age\":19}");
		
		JavaRDD<String> studentInfoRDD = sc.parallelize(studentInfos);
		DataFrame studentInfoDF = sqlContext.read().json(studentInfoRDD);
		
		//注册学生基本信息临时表,并查询分数大80分的学生的基本信息 
		studentInfoDF.registerTempTable("student_info");
		String sql = "select name,age from student_info where name in (";
		for (int i=0; i< goodStudentName.size(); i++) {
			sql +="'" + goodStudentName.get(i) + "'";
			if (i < goodStudentName.size() - 1) {
				sql +=",";
			}
		}
		
		sql +=")";
		
		DataFrame goodStudentInfoDF = sqlContext.sql(sql);
		
		//将两份DataFrame数据关联，转换为JavaPairRDD,执行join transformation
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentPairRDD = goodStudentDF.javaRDD().mapToPair(new PairFunction<Row,String,Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String,Integer>(row.getString(0),
								Integer.valueOf(String.valueOf(row.getLong(1)) ));
			}
			
		}).join(goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row,String,Integer>() {

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return  new Tuple2<String,Integer>(row.getString(0),
						Integer.valueOf(String.valueOf(row.getLong(1)) ));
			}
			
		}));
		
		//将封装在RDD里的好学生的全部信息,转换为一个JavaRDD<Row>的信息
		JavaRDD<Row> goodStudentRowRDD = goodStudentPairRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>,Row>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
					throws Exception {
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
			
		});
		
		//创建一份元数据,将JavaRDD<Row>转换为DataFrame
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);
		
		DataFrame goodStudentAllDF = sqlContext.createDataFrame(goodStudentRowRDD, structType);
		goodStudentAllDF.printSchema();
		goodStudentAllDF.show();
		goodStudentAllDF.write().format("json").save("hdfs://iZ113mzdkz0Z:9000/user/caiyf/good-students");
		
	}

}
