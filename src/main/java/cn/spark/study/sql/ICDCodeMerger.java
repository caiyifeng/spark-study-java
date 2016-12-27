package cn.spark.study.sql;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import cn.spark.study.MedicinalInfo;
import scala.Tuple2;

public class ICDCodeMerger {

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    	final Map<Integer,String> methodMap = new HashMap<Integer,String>();
    	methodMap.put(0, "setIcdCode");
    	methodMap.put(1, "setIcdName");
    	methodMap.put(2, "setIcdRisk");
    	methodMap.put(3, "setIcdAccident");
    	methodMap.put(4, "setIcdDisease");


    	final Map<Integer,String> methodGetMap = new HashMap<Integer,String>();
    	methodGetMap.put(0, "getIcdCode");
    	methodGetMap.put(1, "getIcdName");
    	methodGetMap.put(2, "getIcdRisk");
    	methodGetMap.put(3, "getIcdAccident");
    	methodGetMap.put(4, "getIcdDisease");
 	
		SparkConf conf = new SparkConf().setAppName("MedicinalInfoMerger").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		final String DEFAULT_SEPARATOR = Character.toString((char) 0x01);
		
//		final Field[] flds = HospitalInfo.class.getDeclaredFields();

		JavaRDD<String> hospitalInfoRDD = sc.textFile("hdfs://iZ113mzdkz0Z:9000/user/caiyf/icd_code/icd-code.txt");
		//JavaRDD<String> hospitalInfoRDD = sc.textFile("d://temp//hospital_info.txt");
		
		//final Class<?> bClazz = Class.forName("cn.spark.study.HospitalInfo");
		//HospitalInfo.class
		
		
		JavaPairRDD<String,MedicinalInfo> hospitalInfoRDDNew = hospitalInfoRDD.mapToPair(new PairFunction<String,String,MedicinalInfo>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, MedicinalInfo> call(String line)
					throws Exception {
				String[] lineSplited = line.split(DEFAULT_SEPARATOR);
				String key = lineSplited[1];
				Object bObj = MedicinalInfo.class.newInstance();
				for (int i=0;i<lineSplited.length;i++) {
					String methodName = methodMap.get(i);
					//System.out.println("methodName===========" + methodName);
					
					Method unChangeMethod1 =  MedicinalInfo.class.getDeclaredMethod(methodName, String.class);
					unChangeMethod1.invoke(bObj,lineSplited[i]);					
				}
				MedicinalInfo mdiInfo = (MedicinalInfo) bObj;
				return new Tuple2<String,MedicinalInfo>(key,mdiInfo);
			}
			
		});
		
		//进行合并,并去重
		JavaPairRDD<String, Iterable<MedicinalInfo>> groupByRDD =  hospitalInfoRDDNew.groupByKey();
		
//		List<Tuple2<String,Iterable<MedicinalInfo>>>  groupList =  groupByRDD.collect();
//		for (Tuple2<String,Iterable<MedicinalInfo>> tuple: groupList) {
//			//System.out.println("key=============" + tuple._1);
//			Iterator<MedicinalInfo>  infos = tuple._2.iterator();
//			while(infos.hasNext()) {
//				MedicinalInfo info = infos.next();
//				//System.out.println("name=============" + info.getHospitalName());
//			}
//		}
		
		
		JavaRDD<MedicinalInfo>  mergeInfoRDD = groupByRDD.map(new Function<Tuple2<String,Iterable<MedicinalInfo>>,MedicinalInfo>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public MedicinalInfo call(
					Tuple2<String, Iterable<MedicinalInfo>> hospitalInfos)
					throws Exception {
				String Keyword = hospitalInfos._1;
				Iterator<MedicinalInfo>  infos = hospitalInfos._2.iterator();
				
				MedicinalInfo newHspInfo = new MedicinalInfo();
				while (infos.hasNext()) {
					
					MedicinalInfo hspInfo = infos.next();
//					System.out.println("info=============" + hspInfo.toString());
					newHspInfo.mergeInfo(hspInfo);
				}
				return newHspInfo;
			}
			
		});
		
//		List<HospitalInfo> infoList =   mergeInfoRDD.collect();
//		for(HospitalInfo info : infoList) {
//			System.out.println("info=====aaaaaaaa=====" + info.toString());  
//		}			
		JavaRDD<String> lineRDD = mergeInfoRDD.map(new Function<MedicinalInfo,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			Object bObj = MedicinalInfo.class.newInstance();
			
			@Override
			public String call(MedicinalInfo info) throws Exception {
				String line = ""; 
				Field[] flds = MedicinalInfo.class.getDeclaredFields();
			//	System.out.println("flds.size()=============" + flds.length);
				for (int i=0;i<flds.length;i++) {
					String methodName = methodGetMap.get(i);
					Method unChangeMethod1 =  MedicinalInfo.class.getDeclaredMethod(methodName);
			//		System.out.println("methodName=============" + methodName);
					String fieldValue = (String)unChangeMethod1.invoke(info);
					if (fieldValue ==null || fieldValue.length() ==0)
						fieldValue = "";
			//		System.out.println("fieldValue=============" + fieldValue);
					if (i== 4)
						line = line + fieldValue ;
					else
						line = line + fieldValue + DEFAULT_SEPARATOR;
				}
				return line;
			}
			
		});
		
//		List<String> hspLists =   lineRDD.collect();
//		for(String info : hspLists) {
//			System.out.println("info=====bbbbbbb=====" + info.toString());  
//		}			
//		
		lineRDD.saveAsTextFile("hdfs://iZ113mzdkz0Z:9000/user/caiyf/icdcode_merge");
		
//		List<HospitalInfo> infoList =   mergeInfoRDD.collect();
//		for(HospitalInfo info : infoList) {
//			System.out.println(info.toString());  
//		}		
		sc.close();
		
		
	}
	

}
