package com.jukuad.statistic.device;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ImeiCache {
	
	private static ImeiCache cache = null;
	private static ImeiService adService = new AdImeiServiceImpl();
	private static ImeiService appService = new AppImeiServiceImpl();
	/**imei基础库信息，采集新imei可以快速对比,保留一天**/
	private static Map<String, DBObject> imeiCache = new HashMap<String, DBObject>();
	/**应用：新增用户使用，八天前到昨天的7天imei去重集合**/
	private static Map<String, List<String>> eightExcludeTodayCache = new HashMap<String, List<String>>();
    /**当天imei去重集合：应用和广告**/
	private static Map<String, List<String>> todayAppCache = new HashMap<String, List<String>>();
	/**当天imei去重集合：广告**/
	private static Map<String, List<String>> todayAdCache = new HashMap<String, List<String>>();
	/***当天库保存的imei信息**/
	private static List<String> dayImeiCache = new ArrayList<String>();
	
	public synchronized static ImeiCache getInstance()
	{
		if(cache == null) cache = new ImeiCache();
		return cache;
	}
	
	
	public void clear()
	{
		imeiCache.clear();
		dayImeiCache.clear();
		eightExcludeTodayCache.clear();
	}
	
	public void clearCache()
	{
		imeiCache.clear();
		dayImeiCache.clear();
		eightExcludeTodayCache.clear();
		todayAppCache.clear();
		todayAdCache.clear();
	}
	
	public DBObject getImeiInfo(String imei,String hour)
	{
		if(imeiCache.size() == 0)
		{
			//初始化当天的imei信息
			List<DBObject> list = appService.getOneDayImeis(hour);
			for (DBObject dbObject : list) {
				imeiCache.put(dbObject.get("value").toString(), dbObject);
			}
		}
		return imeiCache.get(imei);
	}
	
	/**
	 * 当天的imei信息是否包含此imei
	 * @param imei
	 * @return
	 */
	public boolean hasImei(String imei,String hour)
	{
		if(imeiCache.size() == 0)
		{
			//初始化当天的imei信息
			List<DBObject> list = appService.getOneDayImeis(hour);
			for (DBObject dbObject : list) {
				imeiCache.put(dbObject.get("value").toString(), dbObject);
			}
		}
		if(imeiCache.containsKey(imei)) return true;
		else return false;
	}
	
	public List<String> getEightExcludeTodayCache(String id,String start,String end)
	{
		if(!eightExcludeTodayCache.containsKey(id))
		{
			eightExcludeTodayCache.put(id, appService.getDistinctImeisBetweenSevenDays(start, end, id));
		}
		return eightExcludeTodayCache.get(id);
	}
	
	
	public List<String> getTodayAppCache(String id,String hour)
	{
		if(!todayAppCache.containsKey(id))
		{
			todayAppCache.put(id, appService.getImeisByOneDay(hour, id));
		}
		return todayAppCache.get(id);
	}
	
	public List<String> getTodayAdCache(String id,String hour)
	{
		if(!todayAdCache.containsKey(id))
		{
			todayAdCache.put(id, adService.getImeisByOneDay(hour, id));
		}
		return todayAdCache.get(id);
	}
	
	public List<String> getDayImeiCache()
	{
		return dayImeiCache;
	}
	
	
	/**
	 * 获取缓存中留存用户个数
	 * @param id
	 * @return
	 */
	public int getTodayAppCache(String id)
	{
		if(todayAppCache.containsKey(id)) return todayAppCache.get(id).size();
		return 0;
	}
	
	public int getTodayAdCache(String id)
	{
		if(todayAdCache.containsKey(id)) return todayAdCache.get(id).size();
		return 0;
	}
	
     public void syncAppHourImei(String id, List<String> hourSave) 
     {
    	 List<String> cacheImei = todayAppCache.get(id);
    	 if(cacheImei.addAll(hourSave)) todayAppCache.put(id, cacheImei);
	 }
     
     public void syncAdHourImei(String id, List<String> hourSave) 
     {
    	 List<String> cacheImei = todayAdCache.get(id);
    	 if(cacheImei.addAll(hourSave)) todayAdCache.put(id, cacheImei);
	 }
     
     public void syncDayImeiCache(List<String> hourSave)
     {
    	 if(!dayImeiCache.addAll(hourSave)){
    		 dayImeiCache.clear();
    		 dayImeiCache = appService.getImeisByOneDay();
    	 }
     }
     
     public void syncImeiCache(List<DBObject> list) 
     {
    	 DBObject obj = null;
    	 String imei = null;
    	 for (DBObject dbObject : list) {
    		 imei = dbObject.get("value").toString();
			if(!imeiCache.containsKey(imei))
			{
				obj = new BasicDBObject();
				obj.put("brand", dbObject.get("brand"));
				obj.put("model", dbObject.get("model"));
				obj.put("net", dbObject.get("net"));
				obj.put("ver", dbObject.get("ver"));
				obj.put("prv", dbObject.get("prv"));
				imeiCache.put(imei, obj);
			}
		}
	 }
}
