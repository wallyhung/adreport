package com.jukuad.statistic.config;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jukuad.statistic.service.StatisticService;
import com.jukuad.statistic.util.TimeUtil;

public class HourJob implements Job
{
	private static final Logger logger = LoggerFactory.getLogger(HourJob.class);

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException 
	{
		logger.info("每小时的日志分析任务启动了....");
		logger.info("Generating report -{}; 调度器执行的时间为： {} ",context.getJobDetail().getFullName(), context.getJobDetail().getJobDataMap().get("hour"));
		
		String hour = TimeUtil.getDayLastHour(new Date());
		StatisticService.hourStatisticAndValidate(hour);
	}
}
