package com.jukuad.statistic.service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.config.MongoDBConnConfig;
import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.log.SoftFeedback;
import com.jukuad.statistic.util.Constant;
import com.jukuad.statistic.util.ImeiParser;
import com.jukuad.statistic.util.LogFileParser;

public class ThreadPool 
{

	private static final Logger logger = LoggerFactory.getLogger(ThreadPool.class);
	private static ExecutorService executor = null;
	
	/**任务类型：
	 * 1.分析请求、推送、展示、点击、下载、安装日志
	 * 2.分析请求用户的imei信息，建立imei信息库
	 */
	private int type;
	
	/**日志所在的路径**/
	private String[] path;
	
	private String hour;
	
	///~ default Constructor
	public ThreadPool() {
	}
	
	public ThreadPool(int type,String[] path,String hour) {
		this.type = type;
		this.path = path;
		this.hour = hour;
	}
	

	public static synchronized ExecutorService getInstance()
	{
		if(executor == null) executor = Executors.newCachedThreadPool();
		return executor;
	}
	
	
	private String getLogPath(String logtype,String root)
	{
		return root + logtype + "/" + hour + ".log";
	}
	

	/**
	 * 检查是否有新的日志文件
	 * @param logtype 日志类型
	 * @param hour 小时字符串
	 * @return
	 */
	private boolean existNewLogs(String logtype,String root)
	{
		boolean bool = false;
		File file = new File(getLogPath(logtype, root));
		if(file.exists()) bool = true;
		return bool;
	}
	
	
	/**
	 * 检查是否有新的日志产生，统计判断
	 * @param logtype
	 * @return
	 */
	public boolean existNewLogs(String logtype)
	{
		boolean bool = false;
		for (String root : path) {
			File file = new File(getLogPath(logtype, root));
			if(file.exists()) bool = true;
			break;
		}
		return bool;
	}
	
	
	
	private List<String> getExistLogPath()
	{
		List<String> list = new ArrayList<String>();
		if(path != null)
		{
			for (String root : path) 
			{
				if(existNewLogs(Constant.PATH_REQUEST, root)) list.add(getLogPath(Constant.PATH_REQUEST, root));
				if(existNewLogs(Constant.PATH_PUSH, root)) list.add(getLogPath(Constant.PATH_PUSH, root));
				if(existNewLogs(Constant.PATH_VIEW, root)) list.add(getLogPath(Constant.PATH_VIEW, root));
				if(existNewLogs(Constant.PATH_CLICK, root)) list.add(getLogPath(Constant.PATH_CLICK, root));
				if(existNewLogs(Constant.PATH_DOWNLOAD, root)) list.add(getLogPath(Constant.PATH_DOWNLOAD, root));
				if(existNewLogs(Constant.PATH_INSTALL, root)) list.add(getLogPath(Constant.PATH_INSTALL, root));
			}
		}
		return list;
	}
	
	private List<String> getExistDeviceLogPath()
	{
		List<String> list = new ArrayList<String>();
		if(path != null)
		{
			for (String root : path) 
			{
				if(existNewLogs(Constant.PATH_REQUEST, root)) list.add(getLogPath(Constant.PATH_REQUEST, root));
			}
		}
		return list;
	}
	
	
	private Runnable generateTask(String logtype,String root,CountDownLatch count)
	{
		Runnable task = null;
		if(existNewLogs(logtype, root))
		{
			if(Constant.PATH_REQUEST.equals(logtype))  task = new LogFileParser<ClientMessage>(ClientMessage.class, getLogPath(logtype, root), count);
			if(Constant.PATH_PUSH.equals(logtype))     task = new LogFileParser<AdFeedback>(AdFeedback.class, getLogPath(logtype, root), count);
			if(Constant.PATH_VIEW.equals(logtype))     task = new LogFileParser<AdFeedback>(AdFeedback.class, getLogPath(logtype, root), count);
			if(Constant.PATH_CLICK.equals(logtype))    task = new LogFileParser<AdFeedback>(AdFeedback.class, getLogPath(logtype, root), count);
			if(Constant.PATH_DOWNLOAD.equals(logtype)) task = new LogFileParser<SoftFeedback>(SoftFeedback.class, getLogPath(logtype, root), count);
			if(Constant.PATH_INSTALL.equals(logtype))  task = new LogFileParser<SoftFeedback>(SoftFeedback.class, getLogPath(logtype, root), count);
		}
		return task;
	}
	
	
	public void init()
	{
		int taskNum = this.getExistLogPath().size();
		if(taskNum > 0)
		{
			//清理缓存数据库
			Datastore ds = MongoDBDataStore.getTemp();
			ds.getMongo().dropDatabase(MongoDBConnConfig.DATABASE_TEMP);
			
			final CountDownLatch count = new CountDownLatch(taskNum);
			//提交taskNum个日志分析任务
			logger.info("{}，{}个日志分析任务将运行。",hour,taskNum);
			
//			ExecutorService executorService = Executors.newCachedThreadPool();
			ExecutorService executorService = Executors.newCachedThreadPool();
			for (String root : path) 
			{
				Runnable requestTask = generateTask(Constant.PATH_REQUEST, root, count);
				if(requestTask != null) executorService.submit(requestTask);
				
				Runnable pushTask = generateTask(Constant.PATH_PUSH, root, count);
				if(pushTask != null) executorService.submit(pushTask);
				
				Runnable viewTask = generateTask(Constant.PATH_VIEW, root, count);
				if(viewTask != null) executorService.submit(viewTask);
				
				Runnable clickTask = generateTask(Constant.PATH_CLICK, root, count);
				if(clickTask != null) executorService.submit(clickTask);
				
				Runnable downTask = generateTask(Constant.PATH_DOWNLOAD, root, count);
				if(downTask != null) executorService.submit(downTask);
				
				Runnable task = generateTask(Constant.PATH_INSTALL, root, count);
				if(task != null) executorService.submit(task);
			}
			try 
			{
				count.await();
				executorService.shutdown();
				executorService = null;
			} catch (InterruptedException e) {
			}
		}
		
	}
	
	
	/**
	 * 创建设备信息线程
	 */
	public void initDevice()
	{
		int taskNum = this.getExistDeviceLogPath().size();
		if(taskNum > 0)
		{
			final CountDownLatch count = new CountDownLatch(taskNum);
			//提交taskNum个日志分析任务
			logger.info("{}，{}个设备日志分析任务将运行。",hour,taskNum);
			
			ExecutorService deviceExecutor = Executors.newCachedThreadPool();
			for (String root : path) 
			{
				if(existNewLogs(Constant.PATH_REQUEST, root))
				{
					deviceExecutor.submit(new ImeiParser(getLogPath(Constant.PATH_REQUEST, root), count));
				}
			}
			try 
			{
				count.await();
				deviceExecutor.shutdown();
				deviceExecutor = null;
			} catch (InterruptedException e) {
			}
		}
	}
	
	

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String[] getPath() {
		return path;
	}

	public void setPath(String[] path) {
		this.path = path;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}
	
	
	
	
	
	
	
}
