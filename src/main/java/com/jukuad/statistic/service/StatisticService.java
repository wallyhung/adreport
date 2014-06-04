package com.jukuad.statistic.service;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.config.MongoDBConnConfig;
import com.jukuad.statistic.device.AppImeiServiceImpl;
import com.jukuad.statistic.device.ImeiCache;
import com.jukuad.statistic.device.ImeiService;
import com.jukuad.statistic.pojo.AdDayStatistic;
import com.jukuad.statistic.pojo.AdResult;
import com.jukuad.statistic.pojo.AppDayStatistic;
import com.jukuad.statistic.pojo.AppResult;
import com.jukuad.statistic.pojo.Click;
import com.jukuad.statistic.pojo.DaySum;
import com.jukuad.statistic.pojo.DeviceDayStatistic;
import com.jukuad.statistic.pojo.Download;
import com.jukuad.statistic.pojo.Install;
import com.jukuad.statistic.pojo.Push;
import com.jukuad.statistic.pojo.Request;
import com.jukuad.statistic.pojo.View;
import com.jukuad.statistic.util.Constant;
import com.jukuad.statistic.util.SecondImeiParser;
import com.jukuad.statistic.util.TimeUtil;

public class StatisticService 
{
	
	private static final Logger logger = LoggerFactory.getLogger(StatisticService.class);
	private static ImeiService deviceService = new AppImeiServiceImpl();
	private static ImeiCache cache = ImeiCache.getInstance();
	/**
	 * 进行日志分析并
	 * 隔一小时统计
	 * @param hour 小时的时间字符串、文件名
	 */
	public static void statistic(String hour)
	{
		logger.info("{}.清理脏数据...",hour);
		MongoService<Request> request = new AppMongService<Request>();
		request.daleteHourData(hour);
		
		logger.info("{}.采集基础数据...",hour);
		LogAnalysisService.analyzeSingleLog(hour);
		SecondLogAnalysisService.analyzeSingleLog(hour);
//		String[] path = {"d:/bin/logs/","z:/"};
//		ThreadPool pool = new ThreadPool(0, path, hour);
//		pool.init();
		logger.info("{}.日志分析任务完成,开始小时统计任务...",hour);
		
		if(request.existNewHourData(Request.class, hour))
		{
			request.executeMapReduce(Request.class, MongoDBConnConfig.DATABASE_TEMP,hour);
		}
		
		MongoService<Push> apppush = new AppMongService<Push>();
		if(apppush.existNewHourData(Push.class, hour))
		{
			MongoService<Push> adpush = new AdMongService<Push>();
			apppush.executeMapReduce(Push.class, MongoDBConnConfig.DATABASE_TEMP,hour);
			adpush.executeMapReduce(Push.class, MongoDBConnConfig.DATABASE_TEMP,hour);
		}
		
		MongoService<View> appview = new AppMongService<View>();
		if(appview.existNewHourData(View.class, hour))
		{
			MongoService<View> adview = new AdMongService<View>();
			appview.executeMapReduce(View.class, MongoDBConnConfig.DATABASE_TEMP,hour);
			adview.executeMapReduce(View.class, MongoDBConnConfig.DATABASE_TEMP,hour);
		}
		
		MongoService<Click> appclick = new AppMongService<Click>();
		if(appclick.existNewHourData(Click.class, hour))
		{
			MongoService<Click> adclick = new AdMongService<Click>();
			appclick.executeAppByAdTypeMapReduce(Click.class, MongoDBConnConfig.DATABASE_TEMP,hour);
			adclick.executeMapReduce(Click.class, MongoDBConnConfig.DATABASE_TEMP,hour);
		}
		
		MongoService<Download> appdown = new AppMongService<Download>();
		if(appdown.existNewHourData(Download.class, hour))
		{
			MongoService<Download> addown = new AdMongService<Download>();
			appdown.executeAppByAdTypeMapReduce(Download.class, MongoDBConnConfig.DATABASE_TEMP,hour);
			addown.executeMapReduce(Download.class, MongoDBConnConfig.DATABASE_TEMP,hour);
		}
		
		MongoService<Install> appins = new AppMongService<Install>();
		if(appins.existNewHourData(Install.class, hour))
		{
			MongoService<Install> adins = new AdMongService<Install>();
			appins.executeAppByAdTypeMapReduce(Install.class, MongoDBConnConfig.DATABASE_TEMP,hour);
			adins.executeMapReduce(Install.class, MongoDBConnConfig.DATABASE_TEMP,hour);
		}
		logger.info("{}.小时统计任务完成了，等待下一次小时统计任务...",hour);
	}
	
	/**
	 * 是否需要补上昨日统计
	 * @param hour
	 */
	public static void validateDayStatistic(String hour)
	{
		Date date = TimeUtil.getDayFromHourString(hour);
		//获取昨天的时间字符串
		Date lastDate = TimeUtil.getLastDay(date);
		String lastDay = TimeUtil.getDay(lastDate); 
		
		logger.info("lastDay:{}",lastDay);
		String[] hourstrs = hour.split("-");
		MongoService<AdDayStatistic> service = new AppMongService<AdDayStatistic>();
		boolean flag = service.validateDayStatistic(lastDay);
		logger.info("flag:" + flag);
		//判断当前小时只有在2-6点时候，数据库木有进行昨天日统计才统计
		int hourNum = Integer.parseInt(hourstrs[3]);
		if(flag && hourNum > 1 && hourNum < 7)
		{
			logger.info("补上{}日的日统计...",TimeUtil.getDay(lastDate));
			//只有补日统计才会有脏数据
			logger.info("清理{}日统计产生的脏数据...",TimeUtil.getDay(lastDate));
			service.daleteDayData(TimeUtil.getDay(lastDate));
			dayStatistic(lastDate);
		}
	}
	
	/**
	 * 是否进行带终端的日统计
	 * @param hour
	 */
	public static void validateDayStatisticAndDevice(String hour)
	{
		Date date = TimeUtil.getDayFromHourString(hour);
		//获取昨天的时间字符串
		Date lastDate = TimeUtil.getLastDay(date);
		String lastDay = TimeUtil.getDay(lastDate); 
		
		logger.info("lastDay:{}",lastDay);
		String[] hourstrs = hour.split("-");
		DeviceDayStatistic data = deviceService.queryLastData(TimeUtil.getDay(lastDate));
		//判断当前小时只有在2-6点时候，数据库木有进行昨天日统计才统计
		int hourNum = Integer.parseInt(hourstrs[3]);
		if(hourNum > 1 && hourNum < 7 && data == null)
		{
			
			logger.info("补上{}日的日统计...",TimeUtil.getDay(lastDate));
			//只有补日统计才会有脏数据
			MongoService<AdDayStatistic> service = new AppMongService<AdDayStatistic>();
			logger.info("清理{}日统计产生的脏数据...",TimeUtil.getDay(lastDate));
			service.daleteDayData(TimeUtil.getDay(lastDate));
			dayStatistic(lastDate);
		}
	}
	
	public static void hourStatisticAndValidate(String hour)
	{
		BaseService<AdResult> service = new BaseServiceImpl<AdResult>();
		AdResult lastData = service.queryLastData(MongoDBConnConfig.DATABASE_STATISTIC);
		if(lastData != null)
		{
			String dataHour = lastData.getHour();
			logger.info("库里最新统计：{}",dataHour);
			List<String> hourArray = TimeUtil.getDistanceTimeHourArray(hour, dataHour);
			if(hourArray.size() == 0) logger.info("当前进行的小时统计为：{},统计已完成，等待下一次小时统计任务...",hour);
			for (String hourStr : hourArray) 
			{
				validateDayStatistic(hourStr);
				logger.info("当前进行的小时统计为：{}",hourStr);
				//判断7点钟时候，清掉终端缓存数据，更新新的一天的缓存
				if(hour.lastIndexOf("-00") > -1) cache.clear();
				if(hour.lastIndexOf("-07") > -1) cache.clearCache();
				analyzeDevice(hourStr);
				statistic(hourStr);
				writeHourData(hourStr);
			}
		}
		else
		{
			//数据库第一次统计数据或出现问题获取不到最新数据
			analyzeDevice(hour);
			statistic(hour);
			writeHourData(hour);
		}
	}
	
	/**
	 * 对一天的数据做统计
	 * @param date
	 */
	public static void dayStatistic(Date date)
	{
		BaseService<AdResult> service = new BaseServiceImpl<AdResult>();
		List<AdResult> list = service.queryTempStatisticList(AdResult.class, MongoDBConnConfig.DATABASE_STATISTIC, date);
		if(list != null && list.size() > 0)
		{
			//首先对应用数据进行map reduce操作
			MongoService<AppResult> appService = new AppMongService<AppResult>();
			MongoService<AdResult> adService = new AdMongService<AdResult>();
			appService.executeTempMapReduce(date);
			adService.executeTempMapReduce(date);
			//为服务器提供计算数据
			appService.fillInWebAppData(MongoDBConnConfig.DATABASE_STATISTIC, date);
	    	//对一天的推送做总统计
			MongoService<AdDayStatistic> adday = new AdMongService<AdDayStatistic>();
			adday.executeDayMapReduce(AdDayStatistic.class, MongoDBConnConfig.DATABASE_STATISTIC, date);
			//为保证数据推送到web，先推送广告和应用数据
			logger.info("推送日统计的应用数据到web端...");
			writeAdDayData(date);
			//对终端做统计
			deviceService.dayStatistic(date);
			//将每天的应用、广告数据推送到web
			logger.info("推送日统计的终端数据到web端...");
			writeDeviceDayData(date);
			//删除垃圾数据
			logger.info("删除7天以前的备份数据...");
			appService.deleteDayBackup(date);
			logger.info("{}日统计完成...",TimeUtil.getDay(date));
		}
		else logger.info("昨天的数据出现异常...");
	}
	
		
	public static void writeHourData(String hour)
	{
		//推送统计结果到web服务器
		MongoService<AppResult> appservice = new AppMongService<AppResult>();
		MongoService<AdResult> adservice = new AdMongService<AdResult>();
		List<AppResult> applist = appservice.queryHourStatistic(AppResult.class, hour);
		List<AdResult> adlist = adservice.queryHourStatistic(AdResult.class, hour);
		PushService pushService = new PushServiceImpl();
		pushService.writeDayDataToMysql(applist, adlist);
	}
	
	
	/**
	 * 为保证数据到服务器，推送广告和应用数据
	 * @param date
	 */
	@SuppressWarnings("unchecked")
	public static void writeAdDayData(Date date)
	{
		//推送统计结果到web服务器
		MongoService<AppDayStatistic> appservice = new AppMongService<AppDayStatistic>();
		MongoService<AdDayStatistic> adservice = new AdMongService<AdDayStatistic>();
		BaseService<DaySum> dayAdService = new BaseServiceImpl<DaySum>();
		
		List<AppDayStatistic> applist = appservice.queryDayStatistic(date);
		List<AdDayStatistic> adlist = adservice.queryDayStatistic(date);
		DaySum daySum = dayAdService.queryDaySum(date);
		
		PushService pushService = new PushServiceImpl();
		pushService.writeDayDataToMysql(applist, adlist, daySum);
	}
	
	/**
	 * 补充推送终端数据
	 * @param date
	 */
	public static void writeDeviceDayData(Date date)
	{
		PushService pushService = new PushServiceImpl();
		//推送终端数据
        List<DeviceDayStatistic> deviceList = deviceService.queryDayStatistic(date);
		pushService.writeDayDataToMysql(deviceList);
	}
	
	/**
	 * 推送所有数据
	 * @param date
	 */
	@SuppressWarnings("unchecked")
	public static void writeDayData(Date date)
	{
		
		//推送统计结果到web服务器
		MongoService<AppDayStatistic> appservice = new AppMongService<AppDayStatistic>();
		MongoService<AdDayStatistic> adservice = new AdMongService<AdDayStatistic>();
		BaseService<DaySum> dayAdService = new BaseServiceImpl<DaySum>();
		
		List<AppDayStatistic> applist = appservice.queryDayStatistic(date);
		List<AdDayStatistic> adlist = adservice.queryDayStatistic(date);
		DaySum daySum = dayAdService.queryDaySum(date);
		
		PushService pushService = new PushServiceImpl();
		pushService.writeDayDataToMysql(applist, adlist, daySum);
		
		//推送终端数据
        List<DeviceDayStatistic> deviceList = deviceService.queryDayStatistic(date);
		pushService.writeDayDataToMysql(deviceList);
	}
	
	
	public static void analyzeDevice(String hour)
	{
		logger.info("{}.设备采集信息开始...",hour);
//		String[] path = {"d:/bin/logs/","z:/"};
//		ThreadPool pool = new ThreadPool(0, path, hour);
//		pool.initDevice();
		SecondImeiParser parser = new SecondImeiParser(LogAnalysisService.getLogPath(Constant.PATH_REQUEST, hour));
		parser.save();
		SecondImeiParser secParser = new SecondImeiParser(SecondLogAnalysisService.getLogPath(Constant.PATH_REQUEST, hour));
		secParser.save();
		logger.info("{}.设备采集信息完成...",hour);
	}
	
	public static void hourStatistic(String hour)
	{
		analyzeDevice(hour);
		statistic(hour);
		writeHourData(hour);
	}
	
	public static void main(String[] args) {
		long s = System.currentTimeMillis();
//		analyzeDevice("2014-05-14-18");
//		hourStatisticAndValidate(TimeUtil.getDayLastHour(new Date()));
//		writeHourData("2014-05-30-07");
//		dayStatistic(new Date());
		hourStatistic("2014-06-04-10");
		System.out.println((System.currentTimeMillis()-s)/1000);
	}
}
