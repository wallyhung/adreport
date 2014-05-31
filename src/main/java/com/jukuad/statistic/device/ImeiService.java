package com.jukuad.statistic.device;

import java.util.Date;
import java.util.List;

import com.jukuad.statistic.pojo.DeviceDayStatistic;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public interface ImeiService {
	/**
	 * 查询多天以上的imei去重
	 * @param flag   应用或者广告标示 1.广告，2.应用
	 * @param start  开始时间
	 * @param end    结束时间
	 * @param id     查询某个广告或应用（或者一天的总应用、总广告情况）
	 * @return
	 */
	public List<String> getDistinctImeisBetweenSevenDays(String start,String end,String id);
	
	/**
	 * 查询多天以上的imei去重总数
	 * 主要为广告日统计和每日统计终端数
	 * @param flag   应用或者广告标示 1.广告，2.应用
	 * @param start  开始时间
	 * @param end    结束时间
	 * @param id     查询某个广告或应用（或者一天的总应用、总广告情况）
	 * @return 
	 */
	public long getDistinctImeisBetweenSevenDaysCount(String start,String end,String id);
	
	/**
	 * 获取一天内小时imei去重
	 * @param flag
	 * @param start
	 * @param end
	 * @param id
	 * @return
	 */
	public List<String> getImeisByOneDay(String start,String end,String id);
	
	/**
	 * 查询该小时以前所有终端
	 * @param hour
	 * @param id
	 * @return
	 */
	public List<String> getImeisByOneDay(String hour,String id);
	
	/**
	 * 从imei库信息查询当天的imei信息
	 * @return
	 */
	public List<String> getImeisByOneDay();
	
	/**
	 * 查询某小时的终端
	 * @param hour
	 * @param id
	 * @return
	 */
	public List<String> getImeisByOneHour(String hour,String id);
	
	/**
	 * 获取一天内所有的留存
	 * @param flag
	 * @param day
	 * @return
	 */
	public List<String> getImeisByOneDay(String day);
	public long getImeisByOneDayCount(String day);
	
	/**
	 * 获取所有的imeis
	 * @return
	 */
	public long getImeisCount();
	public DBCursor getImeis(int offset,int size);
	public DBObject getImei(String imei);
	/**
	 * 加载一天的imei，缓存使用
	 * @param hour
	 * @return
	 */
	public List<DBObject> getOneDayImeis(String hour);
	
	/**
	 * 从每小时的临时库获取的imei信息
	 * @param flag
	 * @param id
	 * @return
	 */
	public List<String> getDistinctImeiFromLog(String id);
	
	/**
	 * 统计每小时的留存用户、新增用户（针对应用）
	 * @param flag
	 * @param id
	 * @param hour
	 * @return
	 */
	public Integer[] statisticHourUserData(String id,String hour);
	
	/**
	 * 统计一天的留存用户（分应用、广告）
	 * @param flag
	 * @param id
	 * @param day
	 * @return
	 */
	public long statisticRemainUserData(String id,String day);
	
	/**
	 * 统计一天的留存用户和新增用户（全部）
	 * @param flag
	 * @param day
	 * @return
	 */
	public Long[] statisticDayUserData(String day);
	
	public DeviceDayStatistic queryLastData(String day);
	public List<DeviceDayStatistic> queryDayStatistic(Date date);
	
	public void executeDayMapReduce(String day,int type);
	public void dayStatistic(Date date);
}
