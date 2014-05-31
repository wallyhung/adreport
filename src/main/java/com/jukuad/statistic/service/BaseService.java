package com.jukuad.statistic.service;

import java.util.Date;
import java.util.List;
import com.jukuad.statistic.pojo.AdResult;
import com.jukuad.statistic.pojo.DaySum;


public interface BaseService<T>
{
	/**
	 * 一天的应用统计查询
	 * @param date
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	List queryDayStatistic(Date date);
	
	/**
	 * 查询一天的总统计
	 * （包括应用的新增数、展示数、广告的推送、点击、下载、安装、终端数等）
	 * @param date
	 * @return
	 */
	DaySum queryDaySum(Date date);
	
	/**
	 * 查询最新的一条广告临时统计数据
	 * @param database
	 * @return
	 */
	AdResult queryLastData(String database);
	
	/**
	 * 验证库最新广告临时数据是否小于小时统计开始时间
	 * @param database
	 * @param startLongTime
	 * @return
	 */
	boolean validateStatisticLastHourLog(String database,long startLongTime);
	

	/**
	 * 验证是否需要重新统计本小时数据
	 * @param hour
	 * @return
	 */
	boolean validateStatisticLastHourLog(String hour);
	
	/**
	 * 查询某一天的所有临时统计数据
	 * @param database
	 * @param date
	 * @return
	 */
	List<T> queryTempStatisticList(Class<T> name,String database,Date date);
	
	/**
	 * 设备数据库是否存在此imei
	 * @param value
	 * @return
	 */
	boolean isExistImei(String value);
}
