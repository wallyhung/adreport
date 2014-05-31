package com.jukuad.statistic.service;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.log.SoftFeedback;
import com.jukuad.statistic.util.Constant;
import com.jukuad.statistic.util.FileParser;
import com.jukuad.statistic.util.LogFileParser;

public class SecondLogAnalysisService 
{
	private static final Logger logger = LoggerFactory.getLogger(SecondLogAnalysisService.class);
	
	//日志文件夹目录
	private static final String logpath = "d:/bin/log/log1/";
	
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
	
	
	public static void analyzeLog(String hour)
	{
		int taskNum = getThreadNum(hour);
		if(taskNum > 0)
		{
			final CountDownLatch count = new CountDownLatch(taskNum);
			//提交taskNum个日志分析任务
			logger.info("{}，{}个日志分析任务将运行。",hour,taskNum);
			
			ExecutorService executor = Executors.newCachedThreadPool();
			if(existNewLogs(Constant.PATH_REQUEST,hour))
			{
				executor.submit(new LogFileParser<ClientMessage>(ClientMessage.class,getLogPath(Constant.PATH_REQUEST, hour),count));
			}
			
//			if(existNewLogs(Constant.PATH_INFO,hour))
//			{
//				executor.submit(new LogFileParser<ClientMessage>(ClientMessage.class,getLogPath(Constant.PATH_INFO, hour),count));
//			}
			
			if(existNewLogs(Constant.PATH_PUSH,hour))
			{
				executor.submit(new LogFileParser<AdFeedback>(AdFeedback.class,getLogPath(Constant.PATH_PUSH, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_VIEW,hour))
			{
				executor.submit(new LogFileParser<AdFeedback>(AdFeedback.class,getLogPath(Constant.PATH_VIEW, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_CLICK,hour))
			{
				executor.submit(new LogFileParser<AdFeedback>(AdFeedback.class,getLogPath(Constant.PATH_CLICK, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_DOWNLOAD,hour))
			{
				executor.submit(new LogFileParser<SoftFeedback>(SoftFeedback.class,getLogPath(Constant.PATH_DOWNLOAD, hour),count));
			}
			
			if(existNewLogs(Constant.PATH_INSTALL,hour))
			{
				executor.submit(new LogFileParser<SoftFeedback>(SoftFeedback.class,getLogPath(Constant.PATH_INSTALL, hour),count));
			}
			
			try 
			{
				count.await();
				executor.shutdown();
			} catch (InterruptedException e) {
			}
		}
	}
	
	public static void analyzeLogThread(String hour)
	{
		int taskNum = getThreadNum(hour);
		if(taskNum > 0)
		{
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
}
