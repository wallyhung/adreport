package com.jukuad.statistic.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.log.SoftFeedback;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Test {
	private static final Logger logger = LoggerFactory.getLogger(Test.class);
	//日志文件夹目录
	private static final String logpath = "d:/bin/logs/";
	
	/**
	 * 检查是否有新的日志文件
	 * @param type 日志类型
	 * @param hour 小时字符串
	 * @return
	 */
	public static boolean existNewLogs(String type,String hour)
	{
		boolean bool = false;
		File file = new File(getLogPath(type, hour));
		if(file.exists()) bool = true;
		return bool;
	}
			
	public static String getLogPath(String type,String hour)
	{
		return logpath + type + "/" + hour + ".log";
	}
	
	public static String getLogPath(String type,String hour,String root)
	{
		return root + type + "/" + hour + ".log";
	}
	
	
	public static void analyzeLog(String hour)
	{
		//清理缓存数据库
		Datastore ds = MongoDBDataStore.getTemp();
		ds.getDB().dropDatabase();
		Parser.insertBatch(1, getLogPath(Constant.PATH_REQUEST, hour));
		Parser.insertBatch(2, getLogPath(Constant.PATH_PUSH, hour));
		Parser.insertBatch(3, getLogPath(Constant.PATH_VIEW, hour));
		Parser.insertBatch(4, getLogPath(Constant.PATH_CLICK, hour));
		Parser.insertBatch(5, getLogPath(Constant.PATH_DOWNLOAD, hour));
		Parser.insertBatch(6, getLogPath(Constant.PATH_INSTALL, hour));
	}
	
	public static int getThreadNum(String hour)
	{
		int num = 0;
		if(existNewLogs(Constant.PATH_REQUEST,hour)) num++;
		
//		if(existNewLogs(Constant.PATH_INFO,hour)) num++;
		
		if(existNewLogs(Constant.PATH_PUSH,hour)) num++;
		
		if(existNewLogs(Constant.PATH_VIEW,hour)) num++;
		
		if(existNewLogs(Constant.PATH_CLICK,hour)) num++;
		
		if(existNewLogs(Constant.PATH_DOWNLOAD,hour)) num++;
		
		if(existNewLogs(Constant.PATH_INSTALL,hour)) num++;
		return num;
	}
	
	public static void analyzeLogThread(String hour)
	{
		int taskNum = getThreadNum(hour);
		if(taskNum > 0)
		{
			//清理缓存数据库
			Datastore ds = MongoDBDataStore.getTemp();
			ds.getDB().dropDatabase();
			final CountDownLatch count = new CountDownLatch(taskNum);
			//提交taskNum个日志分析任务
			logger.info("{}，{}个日志分析任务将运行。",hour,taskNum);
			
//			ExecutorService executor = Executors.newFixedThreadPool(taskNum);
			ExecutorService executor = Executors.newCachedThreadPool();
			
			if(existNewLogs(Constant.PATH_REQUEST,hour))
			{
				executor.submit(new FileParser<ClientMessage>(1,ClientMessage.class,getLogPath(Constant.PATH_REQUEST, hour),count));
			}
			
//			if(existNewLogs(Constant.PATH_INFO,hour))
//			{
//				executor.submit(new LogFileParser<ClientMessage>(ClientMessage.class,getLogPath(Constant.PATH_INFO, hour),count));
//			}
			
			if(existNewLogs(Constant.PATH_PUSH,hour))
			{
				executor.submit(new FileParser<AdFeedback>(2,AdFeedback.class,getLogPath(Constant.PATH_PUSH, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_VIEW,hour))
			{
				executor.submit(new FileParser<AdFeedback>(3,AdFeedback.class,getLogPath(Constant.PATH_VIEW, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_CLICK,hour))
			{
				executor.submit(new FileParser<AdFeedback>(4,AdFeedback.class,getLogPath(Constant.PATH_CLICK, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_DOWNLOAD,hour))
			{
				executor.submit(new FileParser<SoftFeedback>(5,SoftFeedback.class,getLogPath(Constant.PATH_DOWNLOAD, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_INSTALL,hour))
			{
				executor.submit(new FileParser<SoftFeedback>(6,SoftFeedback.class,getLogPath(Constant.PATH_INSTALL, hour),count));
			}
			try 
			{
				count.await();
				executor.shutdownNow();
			} catch (InterruptedException e) {
			}
		}
	}
	
	private static void test()
	{
		long s = System.currentTimeMillis();
		Datastore ds = MongoDBDataStore.getTemp();
		ds.getDB().dropDatabase();
		List<DBObject> list = new ArrayList<DBObject>();
		DBObject o = null;
		for (int i = 0; i < 1000000; i++) {
			o = new BasicDBObject("ip", "ip139" + i);
			o.put("value", "test" + i);
			o.put("prv", "beijing" + 1);
			o.put("i", i);
			o.put("time", System.currentTimeMillis());
			list.add(o);
		}
		ds.getCollection(com.jukuad.statistic.pojo.Test.class).insert(list);
		long e = System.currentTimeMillis();
		System.out.println("insert:" + (e-s)/1000);
		ds.getCollection(com.jukuad.statistic.pojo.Test.class).createIndex(new BasicDBObject("ip",1).append("value", 1).append("time", -1));
		ds.getCollection(com.jukuad.statistic.pojo.Test.class).createIndex(new BasicDBObject("value",1));
		long i = System.currentTimeMillis();
		System.out.println("index:" + (i-e)/1000);
	}
	
	public static void main(String[] args) {
//		Datastore ds = MongoDBDataStore.getTemp();
//		Datastore ds1 = MongoDBDataStore.getData();
//		Datastore ds2 = MongoDBDataStore.getStatistic();
//		Set<String> col1 = ds.getDB().getCollectionNames();
//		for (Iterator iterator = col1.iterator(); iterator.hasNext();) {
//			String string = (String) iterator.next();
//			ds.getDB().getCollection(string).dropIndexes();
//		}
//		
//		Set<String> col2 = ds1.getDB().getCollectionNames();
//		for (Iterator iterator = col2.iterator(); iterator.hasNext();) {
//			String string = (String) iterator.next();
//			ds1.getDB().getCollection(string).dropIndexes();
//		}
//		
//		Set<String> col3 = ds2.getDB().getCollectionNames();
//		for (Iterator iterator = col3.iterator(); iterator.hasNext();) {
//			String string = (String) iterator.next();
//			ds2.getDB().getCollection(string).dropIndexes();
//		}
		
		long s = System.currentTimeMillis();
//		analyzeLog("2014-05-28-07");
//		analyzeLogThread("2014-05-28-07");
		test();
		System.out.println((System.currentTimeMillis() - s)/1000);
//		System.out.println(2751454+263295);
	}
}
