package com.jukuad.statistic.service;

import java.util.Date;
import java.util.List;

public interface MongoService<T>
{
	/**
	 * 请求、、推送、展示
	 * 结果保存临时统计文档中
	 * @param name
	 * @param database  map collection
	 */
	void executeMapReduce(Class<T> name,String database,String hour);
	
	/**
	 * 应用的点击、下载、安装的统计
	 * 统计点击数：先分为cpc和cpa，再在cpa中分积分墙推荐应用和非积分墙推荐应用
	 * 统计下载数：直接在cpa中分积分墙和非积分墙
	 * 统计安装数：直接在cpa中分积分墙和非积分墙
	 * @param name
	 * @param database
	 */
	void executeAppByAdTypeMapReduce(Class<T> name,String database,String hour);
	
	/**
	 * 对某一天的临时数据做汇总map
	 * 结果保存一天的统计文档中
	 * @param date
	 */
	void executeTempMapReduce(Date date);
	
	/**
	 * 查询某天新增的应用数
	 * @param name
	 * @param database
	 * @param date
	 * @return
	 */
	long queryNewAppsCount(String database,Date date);
	
	/**
	 * 统计一天的数据
	 * @param name
	 * @param database
	 */
	void executeDayMapReduce(Class<T> name,String database,Date date);
	
	/**
	 * 查询每小时的统计
	 * @param name
	 * @param hour
	 * @return
	 */
	List<T>  queryHourStatistic(Class<T> name,String hour);
	
	/**
	 * 查询每日的统计结果
	 * @param date
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	List queryDayStatistic(Date date);
	
	/**
	 * 为服务器提供cpc的总数和安装总数
	 * @param database
	 * @param date
	 */
	void fillInWebAppData(String database,Date date);
	
	boolean validateDayStatistic(String day);
	void daleteHourData(String hour);
	void daleteDayData(String day);
	
	/**
	 * 判断某小时是否有数据
	 * @param name
	 * @param hour
	 * @return
	 */
	boolean existNewHourData(Class<T> name,String hour);
	
	/**
	 * 删除备份数据
	 * @param date
	 */
	void deleteDayBackup(Date date);
}
