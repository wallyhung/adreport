package com.jukuad.statistic.config;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.service.StatisticService;
import com.jukuad.statistic.util.TimeUtil;

public class DayJob implements Job
{
	private static final Logger logger = LoggerFactory.getLogger(DayJob.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("每天的日志分析任务启动了....");
		logger.debug("Generating report -{}; 调度器执行的时间为： {} ",context.getJobDetail().getFullName(), context.getJobDetail().getJobDataMap().get("date"));
		
		Date date = TimeUtil.getLastDay(new Date());
		logger.info("当前进行的一天统计为：{}",TimeUtil.getDay(date));
		
		StatisticService.dayStatistic(date);
	}

}
