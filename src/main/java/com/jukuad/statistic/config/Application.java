package com.jukuad.statistic.config;

import java.util.Date;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.util.TimeUtil;

public class Application 
{
	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	private static SchedulerFactory factory = new StdSchedulerFactory();
	
	private static Scheduler hourScheduler;
	private static Scheduler dayScheduler;
	
	static{
		// 创建一个Scheduler
		try {
			hourScheduler = factory.getScheduler();
			JobDetail jobDetail = new JobDetail("hourStatisticJob", "hourJobGroup",HourJob.class);
			jobDetail.getJobDataMap().put("hour", TimeUtil.getDayHour(new Date()));
			
			long startTime = System.currentTimeMillis() + 2*1000L;       
			SimpleTrigger trigger = new SimpleTrigger("hourTrigger",       
								                      "hourTriggerGroup",       
								                       new Date(startTime),       
								                       null,       
								                       SimpleTrigger.REPEAT_INDEFINITELY,       
								                       60*60*1000L);      
			hourScheduler.scheduleJob(jobDetail, trigger);
			
			dayScheduler = factory.getScheduler();
			JobDetail dayDetail = new JobDetail("dayStatisticJob", "dayJobGroup",DayJob.class);
			dayDetail.getJobDataMap().put("date", new Date());
            // 创建一个每天触发一次的触发器
			Trigger dayTrigger = TriggerUtils.makeHourlyTrigger(24);
			dayTrigger.setGroup("dayTriggerGroup");
			dayTrigger.setStartTime(TriggerUtils.getDateOf(0,30,1));
			// 指明trigger的name
			dayTrigger.setName("dayTrigger");
			dayScheduler.scheduleJob(dayDetail, dayTrigger);
			
		} catch (SchedulerException e) {
			logger.error("schedule", e);
		}
	}
	
	public void start()
	{
		try {
			hourScheduler.start();
			dayScheduler.start();
		} catch (SchedulerException e) {
			logger.error("start schedule", e);
		}
	}
	
	public void stop()
	{
		try {
			if(!hourScheduler.isShutdown())
				hourScheduler.shutdown();
			if(!dayScheduler.isShutdown())
			    dayScheduler.shutdown();
		} catch (SchedulerException e) {
			logger.error("stop schedule", e);
		}
	}
	
	public static void main(String[] args) 
	{
		Application app = new Application();
		app.start();
	}
}
