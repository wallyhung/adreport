package com.jukuad.statistic.device;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;

import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.pojo.DeviceDayStatistic;
import com.jukuad.statistic.pojo.Push;
import com.jukuad.statistic.util.ObjectUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
import com.mysql.jdbc.StringUtils;

public class AdImeiServiceImpl implements ImeiService{
	private ImeiCache cache = ImeiCache.getInstance();
	
	@SuppressWarnings("unchecked")
	@Override
	public List<String> getDistinctImeisBetweenSevenDays(String start, String end, String id) {
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        BasicDBObject query = new BasicDBObject(); 
        query.append("hour", new BasicDBObject().append("$gte", start).append("$lte", end));
        if(!StringUtils.isNullOrEmpty(id))  query.append("adid", id);
        
        List<String> list = new ArrayList<String>();
        try {
        	list = ds.getCollection(AdHourImei.class).distinct("value", query);
		} catch (Exception e) {
			//exception: distinct too big, 16mb cap
			String map = "function(){" 
		               + "emit(this.value, {count:1})"
					   + "}";
			
			String reduce = "function (key, values) {"
						  + "var res = {count:0};"
						  + "for (var i = 0; i < values.length; i++) {"
							    + "res.count += values[i].count; "
							+"}"
							+"return res;"
						+"}";
			String outputCollection = "tmp";
		  //获取数据库一个实例
		  MapReduceOutput out = ds.getCollection(AdHourImei.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  list = analyzeMapReduceResult(out,list);
		}
		return list;
	}
	
	
	@Override
	public long getDistinctImeisBetweenSevenDaysCount(String start,
			String end, String id) 
	{
		long count = 0;
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        BasicDBObject query = new BasicDBObject(); 
        query.append("hour", new BasicDBObject().append("$gte", start).append("$lte", end));
        if(!StringUtils.isNullOrEmpty(id))  query.append("adid", id);
        
        try {
        	count = ds.getCollection(AdHourImei.class).distinct("value", query).size();
		} catch (Exception e) {
			//exception: distinct too big, 16mb cap
			String map = "function(){" 
		               + "emit(this.value, {count:1})"
					   + "}";
			
			String reduce = "function (key, values) {"
						  + "var res = {count:0};"
						  + "for (var i = 0; i < values.length; i++) {"
							    + "res.count += values[i].count; "
							+"}"
							+"return res;"
						+"}";
			String outputCollection = "tmp";
		  //获取数据库一个实例
		  MapReduceOutput out = ds.getCollection(AdHourImei.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  count = out.getOutputCollection().count();
		}
		return count;
	}

	/**
	 * 解析imei去重的结果
	 * @param out
	 * @param list
	 * @return
	 */
	private List<String> analyzeMapReduceResult(MapReduceOutput out,List<String> list) 
	{
		DBCollection dc = out.getOutputCollection();
        DBCursor cursor = dc.find();
        while (cursor.hasNext()) 
        {
			DBObject dbObject = (DBObject) cursor.next();
			String imei =  (String) dbObject.get("_id");
			list.add(imei);
		}
        return list;
	}
	
	@Override
	public List<String> getImeisByOneDay(String start, String end,
			String id) {
		List<String> res = new ArrayList<String>();
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        Query<AdHourImei> query = ds.createQuery(AdHourImei.class);
        query.field("hour").greaterThanOrEq(start).field("hour").lessThanOrEq(end);
        if(!StringUtils.isNullOrEmpty(id))  query.field("adid").equal(id);
        query.retrievedFields(true, "value");
        for (AdHourImei hourImei : query) {
			res.add(hourImei.getValue());
		}
		return res;
	}
	
	@Override
	public List<String> getImeisByOneDay(String hour, String id) {
		List<String> res = new ArrayList<String>();
		//获取数据库一个实例
		hour = hour.substring(0, 10);
        Datastore ds = MongoDBDataStore.getData();
        Query<AdHourImei> query = ds.createQuery(AdHourImei.class);
        query.field("hour").contains(hour);
        if(!StringUtils.isNullOrEmpty(id))  query.field("adid").equal(id);
        query.retrievedFields(true, "value");
        for (AdHourImei hourImei : query) {
			res.add(hourImei.getValue());
		}
		return res;
	}
	
	
	@Override
	public List<String> getImeisByOneHour(String hour, String id) {
		List<String> res = new ArrayList<String>();
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        Query<AdHourImei> query = ds.createQuery(AdHourImei.class);
        if(!StringUtils.isNullOrEmpty(id))  query.field("adid").equal(id);
        query.field("hour").equals(hour);
        query.retrievedFields(true, "value");
        for (AdHourImei hourImei : query) {
			res.add(hourImei.getValue());
		}
		return res;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public List<String> getImeisByOneDay(String day) {
		List<String> res = new ArrayList<String>();
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        BasicDBObject query = new BasicDBObject(); 
        query.append("hour", new BasicDBObject().append("$regex", day));
        try {
        	res = ds.getCollection(AdHourImei.class).distinct("value", query);
		} catch (Exception e) {
			//exception: distinct too big, 16mb cap
			String map = "function(){" 
		               + "emit(this.value, {count:1})"
					   + "}";
			
			String reduce = "function (key, values) {"
						  + "var res = {count:0};"
						  + "for (var i = 0; i < values.length; i++) {"
							    + "res.count += values[i].count; "
							+"}"
							+"return res;"
						+"}";
			String outputCollection = "tmp";
		  //获取数据库一个实例
		  MapReduceOutput out = ds.getCollection(AdHourImei.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  res = analyzeMapReduceResult(out, res);
		}
        return res;
	}
	
	@Override
	public long getImeisByOneDayCount(String day) {
		long count = 0;
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        BasicDBObject query = new BasicDBObject(); 
        query.append("hour", new BasicDBObject().append("$regex", day));
        try {
        	count = ds.getCollection(AdHourImei.class).distinct("value", query).size();
		} catch (Exception e) {
			//exception: distinct too big, 16mb cap
			String map = "function(){" 
		               + "emit(this.value, {count:1})"
					   + "}";
			
			String reduce = "function (key, values) {"
						  + "var res = {count:0};"
						  + "for (var i = 0; i < values.length; i++) {"
							    + "res.count += values[i].count; "
							+"}"
							+"return res;"
						+"}";
			String outputCollection = "tmp";
		  //获取数据库一个实例
		  MapReduceOutput out = ds.getCollection(AdHourImei.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  count = out.getOutputCollection().count();
		}
		return count;
	}
	
	
	
	@SuppressWarnings("unchecked")
	@Override
	public List<String> getDistinctImeiFromLog(String id) {
		List<String> list = new ArrayList<String>();
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getTemp();
        //获取当前小时的imei
        BasicDBObject query = new BasicDBObject();
        query.append("adid", id);
        try {
        	list = ds.getCollection(Push.class).distinct("imei", query);
		} catch (Exception e) {
			//exception: distinct too big, 16mb cap
			String map = "function(){" 
		               + "emit(this.imei, {count:1})"
					   + "}";
			
			String reduce = "function (key, values) {"
						  + "var res = {count:0};"
						  + "for (var i = 0; i < values.length; i++) {"
							    + "res.count += values[i].count; "
							+"}"
							+"return res;"
						+"}";
			String outputCollection = "tmp";
		  //获取数据库一个实例
		  MapReduceOutput out = ds.getCollection(Push.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  list = analyzeMapReduceResult(out, list);
		}
        return list;
	}
	
	
	private void saveHourImei(List<String> hourSave,String hour,String id)
	{
		Datastore ds = MongoDBDataStore.getData();
//		AdHourImei imei = null;
//		for (String value : hourSave) {
//			imei = new AdHourImei(value);
//			imei.setAdid(id);
//			imei.setHour(hour);
//			ds.save(imei);
//		}
		
		DBObject object = null;
		List<DBObject> dblist = new ArrayList<DBObject>(hourSave.size());
		for (String value : hourSave) {
			object = new BasicDBObject();
			object.put("adid", id);
			object.put("hour", hour);
			object.put("value", value);
			dblist.add(object);
		}
		ds.getCollection(AdHourImei.class).insert(dblist);
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Integer[] statisticHourUserData(String id, String hour) {
		Integer[] size = new Integer[2];
		List<String> hourImeis = getDistinctImeiFromLog(id);
		size[0] = hourImeis.size();
		
		//保存一天中前几小时木有的数据到库中  2014-05-15-07
		List<String> cachImeis = cache.getTodayAdCache(id, hour);
		List<String> hourSave = ObjectUtil.getDiffList(cachImeis, hourImeis);
		saveHourImei(hourSave, hour, id);
		//更新缓存
		cache.syncAdHourImei(id,hourSave);
		return size;
	}
	
	@Override
	public long statisticRemainUserData(String id, String day) {
		long size = 0;
		size = cache.getTodayAdCache(id);
		if(size == 0)
		{
			Datastore ds = MongoDBDataStore.getData();
			Query<AdHourImei> query = ds.createQuery(AdHourImei.class);
			query.field("adid").equal(id).field("hour").contains(day);
			size = query.countAll();
		}
		return size;
	}
	
	@Override
	public Long[] statisticDayUserData(String day) {
		Long[] size = new Long[3];
		List<String> res = getImeisByOneDay(day);
		size[0] = (long) res.size();
		return size;
	}


	@Override
	public long getImeisCount() {
		return 0;
	}

	@Override
	public DBCursor getImeis(int offset, int size) {
		return null;
	}

	@Override
	public DBObject getImei(String imei) {
		return null;
	}
	
	@Override
	public List<DBObject> getOneDayImeis(String hour) {
		return null;
	}

	@Override
	public void executeDayMapReduce(String day, int type) {
	}
	
	@Override
	public void dayStatistic(Date date) {
		
	}
	
	@Override
	public DeviceDayStatistic queryLastData(String day) {
		return null;
	}
	
	@Override
	public List<DeviceDayStatistic> queryDayStatistic(Date date) {
		return null;
	}
	@Override
	public List<String> getImeisByOneDay() {
		return null;
	}
	
}
	
