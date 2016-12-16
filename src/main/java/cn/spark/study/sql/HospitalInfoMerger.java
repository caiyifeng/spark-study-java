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

import cn.spark.study.HospitalInfo;
import scala.Tuple2;

public class HospitalInfoMerger {

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    	final Map<Integer,String> methodMap = new HashMap<Integer,String>();
    	methodMap.put(0, "setHospitalName");
    	methodMap.put(1, "setHospitalType");
    	methodMap.put(2, "setHospitalClass");
    	methodMap.put(3, "setHospitalLevel");
    	methodMap.put(4, "setHospitalCapital");
    	methodMap.put(5, "setCompanySpecial");
    	methodMap.put(6, "setInsuranceSpecial");
    	methodMap.put(7, "setProvince");
    	methodMap.put(8, "setCity");
    	methodMap.put(9, "setAddress");
    	methodMap.put(10, "setContact");
    	methodMap.put(11, "setContactInfo");

    	final Map<Integer,String> methodGetMap = new HashMap<Integer,String>();
    	methodGetMap.put(0, "getHospitalName");
    	methodGetMap.put(1, "getHospitalType");
    	methodGetMap.put(2, "getHospitalClass");
    	methodGetMap.put(3, "getHospitalLevel");
    	methodGetMap.put(4, "getHospitalCapital");
    	methodGetMap.put(5, "getCompanySpecial");
    	methodGetMap.put(6, "getInsuranceSpecial");
    	methodGetMap.put(7, "getProvince");
    	methodGetMap.put(8, "getCity");
    	methodGetMap.put(9, "getAddress");
    	methodGetMap.put(10, "getContact");
    	methodGetMap.put(11, "getContactInfo");    	
		SparkConf conf = new SparkConf().setAppName("HospitalInfoMerger").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		final String DEFAULT_SEPARATOR = Character.toString((char) 0x01);
		
//		final Field[] flds = HospitalInfo.class.getDeclaredFields();

		JavaRDD<String> hospitalInfoRDD = sc.textFile("hdfs://iZ113mzdkz0Z:9000/user/caiyf/hospital/hospital_info.txt");
		//JavaRDD<String> hospitalInfoRDD = sc.textFile("d://temp//hospital_info.txt");
		
		//final Class<?> bClazz = Class.forName("cn.spark.study.HospitalInfo");
		//HospitalInfo.class
		
		
		JavaPairRDD<String,HospitalInfo> hospitalInfoRDDNew = hospitalInfoRDD.mapToPair(new PairFunction<String,String,HospitalInfo>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, HospitalInfo> call(String line)
					throws Exception {
				//System.out.println("line==============" + line);
				String[] lineSplited = line.split(DEFAULT_SEPARATOR);
				String key = lineSplited[0];
				Object bObj = HospitalInfo.class.newInstance();
				for (int i=0;i<lineSplited.length;i++) {
					String methodName = methodMap.get(i);
					//System.out.println("methodName===========" + methodName);
					
					Method unChangeMethod1 =  HospitalInfo.class.getDeclaredMethod(methodName, String.class);
					unChangeMethod1.invoke(bObj,lineSplited[i]);					
				}
				HospitalInfo hspInfo = (HospitalInfo) bObj;
				return new Tuple2<String,HospitalInfo>(key,hspInfo);
			}
			
		});
		
		//进行合并,并去重
		JavaPairRDD<String, Iterable<HospitalInfo>> groupByRDD =  hospitalInfoRDDNew.groupByKey();
		
		List<Tuple2<String,Iterable<HospitalInfo>>>  groupList =  groupByRDD.collect();
		for (Tuple2<String,Iterable<HospitalInfo>> tuple: groupList) {
			//System.out.println("key=============" + tuple._1);
			Iterator<HospitalInfo>  infos = tuple._2.iterator();
			while(infos.hasNext()) {
				HospitalInfo info = infos.next();
				//System.out.println("name=============" + info.getHospitalName());
			}
		}
		
		
		JavaRDD<HospitalInfo>  mergeInfoRDD = groupByRDD.map(new Function<Tuple2<String,Iterable<HospitalInfo>>,HospitalInfo>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HospitalInfo call(
					Tuple2<String, Iterable<HospitalInfo>> hospitalInfos)
					throws Exception {
				String Keyword = hospitalInfos._1;
				Iterator<HospitalInfo>  infos = hospitalInfos._2.iterator();
				
				HospitalInfo newHspInfo = new HospitalInfo();
				while (infos.hasNext()) {
					
					HospitalInfo hspInfo = infos.next();
//					System.out.println("info=============" + hspInfo.toString());
					newHspInfo.mergeInfo(hspInfo);
				}
				return newHspInfo;
			}
			
		});
		
		List<HospitalInfo> infoList =   mergeInfoRDD.collect();
		for(HospitalInfo info : infoList) {
			System.out.println("info=====aaaaaaaa=====" + info.toString());  
		}			
		JavaRDD<String> lineRDD = mergeInfoRDD.map(new Function<HospitalInfo,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			Object bObj = HospitalInfo.class.newInstance();
			
			@Override
			public String call(HospitalInfo info) throws Exception {
				String line = ""; 
				Field[] flds = HospitalInfo.class.getDeclaredFields();
				System.out.println("flds.size()=============" + flds.length);
				for (int i=0;i<flds.length;i++) {
					String methodName = methodGetMap.get(i);
					Method unChangeMethod1 =  HospitalInfo.class.getDeclaredMethod(methodName);
					System.out.println("methodName=============" + methodName);
					String fieldValue = (String)unChangeMethod1.invoke(info);
					if (fieldValue ==null || fieldValue.length() ==0)
						fieldValue = "";
					System.out.println("fieldValue=============" + fieldValue);
					if (i== 11)
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
		lineRDD.saveAsTextFile("hdfs://AY131210145836355bbcZ:9000/user/caiyf/hospital_merge");
		
//		List<HospitalInfo> infoList =   mergeInfoRDD.collect();
//		for(HospitalInfo info : infoList) {
//			System.out.println(info.toString());  
//		}		
		sc.close();
		
		
	}
	

}
