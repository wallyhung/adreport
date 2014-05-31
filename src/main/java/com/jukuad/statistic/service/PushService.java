package com.jukuad.statistic.service;

import java.util.List;

import com.jukuad.statistic.pojo.AdDayStatistic;
import com.jukuad.statistic.pojo.AdResult;
import com.jukuad.statistic.pojo.AppDayStatistic;
import com.jukuad.statistic.pojo.AppResult;
import com.jukuad.statistic.pojo.DaySum;
import com.jukuad.statistic.pojo.DeviceDayStatistic;

public interface PushService 
{
	
	/**
	 * 保存每天的总数据
	 * @param sum
	 */
	void writeDaySumToMysql(DaySum sum);
	/**
	 * 一次解析三条数据
	 * @param applist
	 * @param adlist
	 * @param sum
	 */
	void writeDayDataToMysql(List<AppDayStatistic> applist,List<AdDayStatistic> adlist,DaySum sum);

	/**
	 * 推送每小时的广告和应用数据
	 * @param applist
	 * @param adlist
	 */
	void writeDayDataToMysql(List<AppResult> applist, List<AdResult> adlist);
	
	/**
	 * 推送一天的终端统计
	 * @param deviceList
	 */
	void writeDayDataToMysql(List<DeviceDayStatistic> deviceList);
}
