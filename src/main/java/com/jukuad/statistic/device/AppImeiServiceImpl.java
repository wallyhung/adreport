package com.jukuad.statistic.device;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.pojo.DeviceDayStatistic;
import com.jukuad.statistic.pojo.Imei;
import com.jukuad.statistic.pojo.Request;
import com.jukuad.statistic.util.Constant;
import com.jukuad.statistic.util.ObjectUtil;
import com.jukuad.statistic.util.TimeUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
import com.mysql.jdbc.StringUtils;

public class AppImeiServiceImpl implements ImeiService{
	private static final Logger logger = LoggerFactory.getLogger(AppImeiServiceImpl.class);
	private ImeiCache cache = ImeiCache.getInstance();
	
	@SuppressWarnings("unchecked")
	@Override
	public List<String> getDistinctImeisBetweenSevenDays(String start, String end, String id) {
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        BasicDBObject query = new BasicDBObject(); 
        query.append("hour", new BasicDBObject().append("$gte", start).append("$lte", end));
        if(!StringUtils.isNullOrEmpty(id))  query.append("fid", id);
        
        List<String> list = new ArrayList<String>();
        try {
        	list = ds.getCollection(AppHourImei.class).distinct("value", query);
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
		  MapReduceOutput out = ds.getCollection(AppHourImei.class).mapReduce(map, reduce, outputCollection, query);
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
        if(!StringUtils.isNullOrEmpty(id))  query.append("fid", id);
        try {
        	count = ds.getCollection(AppHourImei.class).distinct("value", query).size();
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
		  MapReduceOutput out = ds.getCollection(AppHourImei.class).mapReduce(map, reduce, outputCollection, query);
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
        Query<AppHourImei> query = ds.createQuery(AppHourImei.class);
        query.field("hour").greaterThanOrEq(start).field("hour").lessThanOrEq(end);
        if(!StringUtils.isNullOrEmpty(id)) query.field("fid").equal(id);
        query.retrievedFields(true, "value");
        for (AppHourImei hourImei : query) {
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
        Query<AppHourImei> query = ds.createQuery(AppHourImei.class);
        query.field("hour").contains(hour);
        if(!StringUtils.isNullOrEmpty(id)) query.field("fid").equal(id);
        query.retrievedFields(true, "value");
        for (AppHourImei hourImei : query) {
			res.add(hourImei.getValue());
		}
		return res;
	}
	
	@Override
	public List<String> getImeisByOneDay() {
		//取当前天
		Date date = new Date();
		long s = TimeUtil.getDayStart(date).getTime();
		long e = TimeUtil.getDayEnd(date).getTime();
		
		Datastore ds = MongoDBDataStore.getData();
        Query<Imei> query = ds.createQuery(Imei.class);
        query.field("time").greaterThanOrEq(s).field("time").lessThanOrEq(e);
        query.retrievedFields(true, "value");
        List<String> res = new ArrayList<String>((int)query.countAll());
        for (Imei imei : query) {
			res.add(imei.getValue());
		}
		return res;
	}
	
	@Override
	public List<String> getImeisByOneHour(String hour, String id) {
		List<String> res = new ArrayList<String>();
		//获取数据库一个实例
        Datastore ds = MongoDBDataStore.getData();
        Query<AppHourImei> query = ds.createQuery(AppHourImei.class);
        if(!StringUtils.isNullOrEmpty(id))  query.field("fid").equal(id);
        query.field("hour").equals(hour);
        query.retrievedFields(true, "value");
        for (AppHourImei hourImei : query) {
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
        	res = ds.getCollection(AppHourImei.class).distinct("value", query);
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
		  MapReduceOutput out = ds.getCollection(AppHourImei.class).mapReduce(map, reduce, outputCollection, query);
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
        	count = ds.getCollection(AppHourImei.class).distinct("value", query).size();
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
		  MapReduceOutput out = ds.getCollection(AppHourImei.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  count = out.getOutputCollection().count();
		}
		return count;
	}
	
	@Override
	public long getImeisCount() {
		Datastore ds = MongoDBDataStore.getData();
		return ds.createQuery(Imei.class).countAll();
	}
	
	@Override
	public DBCursor getImeis(int offset,int size) {
		Datastore ds = MongoDBDataStore.getData();
		//只返回部分属性
//		Query<Imei> query = ds.createQuery(Imei.class).retrievedFields(false, "_id")
//				                                      .retrievedFields(false, "ip")
//				                                      .retrievedFields(false, "mos")
//				                                      .retrievedFields(false, "loc")
//				                                      .retrievedFields(false, "ua")
//				                                      .retrievedFields(false, "time");
//		//跳过offset条返回size条document
//		query.offset(offset).limit(size);
		BasicDBObject keys = new BasicDBObject();
//		keys.append("_id", 0).append("value", 1).append("brand", 1).append("model", 1).append("net", 1)
//		      .append("ver", 1).append("prv", 1);
		keys.append("_id", 0).append("value", 1).append("prv", 1);
		DBCursor cursor = ds.getCollection(Imei.class).find(null, keys).skip(offset).limit(size);
		return cursor;
	}
	
	public List<String> writeImeiData()
	{
		int page_size = 100000;
		long count = getImeisCount();
		List<String> arrs = new ArrayList<String>((int) count);
		StringBuilder sb = new StringBuilder();
		int amount = 0;
		DBCursor cursor = null;
		do {
			cursor = getImeis(amount, page_size);
			while (cursor.hasNext()) {
				DBObject object = cursor.next();
				sb.append(object.get("value")).append("|").append(object.get("prv"));
				arrs.add(sb.toString());
				sb = new StringBuilder();
			}
			amount += page_size;
		} while (amount < count);
		return arrs;
	}
	
	
	private static void generate(List<String> arrs)
	{
//		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			File file = new File("d:/imei-province.txt");
			if(file.exists()) file.delete();
//			fw = new FileWriter(new File(path));
//			BufferedWriter bw = new BufferedWriter(fw);
			bw = new BufferedWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8")));
			for(String arr : arrs){
	            bw.write(arr+"\t\n");
	        }
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }finally
        {
        	try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
	
	@Override
	public DBObject getImei(String imei) {
		Datastore ds = MongoDBDataStore.getData();
		BasicDBObject query = new BasicDBObject();
		query.append("value", imei);
		
		BasicDBObject keys = new BasicDBObject();
		keys.append("_id", 0).append("brand", 1).append("model", 1)
		    .append("net", 1).append("ver", 1).append("prv", 1);
		return ds.getCollection(Imei.class).findOne(query, keys);
	}
	
	@Override
	public List<DBObject> getOneDayImeis(String hour) {
		//取当前天
		Date date = TimeUtil.getDayFromHourString(hour);
		long s = TimeUtil.getDayStart(date).getTime();
		long e = TimeUtil.getDayEnd(date).getTime();
		
		Datastore ds = MongoDBDataStore.getData();
		BasicDBObject query = new BasicDBObject();
		query.append("time", new BasicDBObject().append("$gte", s).append("$lte", e));
		BasicDBObject keys = new BasicDBObject();
		keys.append("_id", 0).append("brand", 1).append("model", 1)
		    .append("net", 1).append("ver", 1).append("prv", 1).append("value", 1);
		return ds.getCollection(Imei.class).find(query, keys).toArray();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<String> getDistinctImeiFromLog(String id) {
		//获取数据库一个实例
		List<String> list = new ArrayList<String>();
        Datastore ds = MongoDBDataStore.getTemp();
        //获取当前小时的imei
        BasicDBObject query = new BasicDBObject();
        if(!StringUtils.isNullOrEmpty(id))  query.append("fid", id);
        try {
        	list = ds.getCollection(Request.class).distinct("imei", query);
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
		  MapReduceOutput out = ds.getCollection(Request.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		  list = analyzeMapReduceResult(out, list);
		}
        return list;
	}
	
	
	private void saveHourImei(List<String> hourSave,String hour,String id)
	{
		Datastore ds = MongoDBDataStore.getData();
		DBObject object = null;
		List<DBObject> dblist = new ArrayList<DBObject>(hourSave.size());
		for (String value : hourSave) {
			object = new BasicDBObject();
			object.put("fid", id);
			object.put("hour", hour);
			object.put("value", value);
			object.put("imei", cache.getImeiInfo(value,hour));
			dblist.add(object);
		}
		ds.getCollection(AppHourImei.class).insert(dblist);
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Integer[] statisticHourUserData(String id, String hour) {
		Integer[] size = new Integer[2];
		List<String> hourImeis = getDistinctImeiFromLog(id);
		size[0] = hourImeis.size();
		
		//保存一天中前几小时木有的数据到库中  2014-05-15-07
		List<String> cachImeis = cache.getTodayAppCache(id, hour);
		List<String> hourSave = ObjectUtil.getDiffList(cachImeis, hourImeis);
		saveHourImei(hourSave, hour, id);
		//更新缓存
		cache.syncAppHourImei(id,hourSave);
		//先获取当天的上一天到前8天的留存用户     统计的前一天
        Date date = TimeUtil.getDayFromHourString(hour);
		Date before = TimeUtil.getEightDaysBeforeStart(date);
		Date last = TimeUtil.getLastDayEnd(date);
		List<String> eightImei = cache.getEightExcludeTodayCache(id, TimeUtil.getDayHour(before), TimeUtil.getDayHour(last));
		size[1] = ObjectUtil.getDiffSize(eightImei, hourSave);
		return size;
	}
	
	@Override
	public long statisticRemainUserData(String id, String day) {
		long size = 0;
		size = cache.getTodayAppCache(id);
		if(size == 0)
		{
			Datastore ds = MongoDBDataStore.getData();
			Query<AppHourImei> query = ds.createQuery(AppHourImei.class);
			query.field("fid").equal(id).field("hour").contains(day);
			size = query.countAll();
		}
		return size;
	}
	
	@Override
	public Long[] statisticDayUserData(String day) {
		Long[] size = new Long[3];
		List<String> res = getImeisByOneDay(day);
		size[0] = (long) res.size();
		
		//先获取当天的上一天到前8天的留存用户     统计的前一天
        Date date = TimeUtil.getDayFromDayString(day);
		Date before = TimeUtil.getEightDaysBeforeStart(date);
		Date last = TimeUtil.getLastDayEnd(date);
		String start = TimeUtil.getDayHour(before);
		String end = TimeUtil.getDayHour(last);
		
		List<String> eight = getDistinctImeisBetweenSevenDays(start, end, null);
		size[1] = (long) ObjectUtil.getDiffSize(eight, res);
		return size;
	}
	
	/**
	 * 每日的终端统计
	 * @param date 统计日期
	 * @param type 统计类型
	 */
	public void executeDayMapReduce(String day,int type)
	{
		String field = null;
		switch (type) {
		case Constant.REPORT_TYPE_BRAND:
			field = "brand"; //品牌
			break;
		case Constant.REPORT_TYPE_LOCATION:
			field = "prv"; //省份
			break;
		case Constant.REPORT_TYPE_NET:
			field = "net"; //联网方式
			break;
		case Constant.REPORT_TYPE_MODEL:
			field = "model"; //型号
			break;
		case Constant.REPORT_TYPE_OS:
			field = "ver";  //系统版本号
			break;
		default:
			break;
		}
		
		
		String map = "function(){" 
	               + "if(this.imei!= null) var key = this.imei." + field + ";"
		           + " emit(key,{count: 1}); "
				   + "}";
		
		String reduce = "function (key, values) {"
					  + "var res = {count : 0};"
					  + "values.forEach(function(val) {"
						  + "res.count += val.count; "
						  +"});"
					  +"return res;"
					+"}";
		String outputCollection = "tmp";
		  //获取数据库一个实例
		Datastore ds = MongoDBDataStore.getData();  
		BasicDBObject query = new BasicDBObject();
		query.append("hour", new BasicDBObject().append("$regex", day));
		MapReduceOutput out = ds.getCollection(AppHourImei.class).mapReduce(map, reduce, outputCollection, query);
		  //处理查询结果并入库
		analyzeDeviceMapReduceResult(out,day,type);
	}
	
	
	
	private void analyzeDeviceMapReduceResult(MapReduceOutput out,String day,int type) 
	{
		DBCollection dc = out.getOutputCollection();
        DBCursor cursor = dc.find().sort(new BasicDBObject("value.count", -1));
        //获取一个数据库实例
        Datastore ds = MongoDBDataStore.getStatistic();
        logger.info("终端统计结果，mapreduce执行状况：{}",dc.getStats());
        
        int num = 0;
	    int othersum = 0;
        while (cursor.hasNext()) 
        {
			DBObject dbObject = (DBObject) cursor.next();
			DeviceDayStatistic res = new DeviceDayStatistic();
			String id =  (String) dbObject.get("_id");
			
			DBObject valueOb = (DBObject) dbObject.get("value");
			Double count = (Double) valueOb.get("count");
			
			//取排名10以后的字段为other
			if(num > 19 && type != Constant.REPORT_TYPE_LOCATION)
			{
				othersum += count;
			}
			else
			{
				res.setField(id);
				res.setCount(count.longValue());
				res.setDay(day);
				res.setType(type);
				Key<DeviceDayStatistic> key = ds.save(res);
				logger.debug("分析统计结果，并入库：{}",key.getId());
			}
			num++;
		}
        
        if(othersum > 0)
        {
        	DeviceDayStatistic other = new DeviceDayStatistic();
            other.setField("other");
            other.setCount(othersum);
            other.setDay(day);
            other.setType(type);
    		Key<DeviceDayStatistic> key = ds.save(other);
    		logger.debug("分析统计结果，并入库：{}",key.getId());
        }
	}
	
	@Override
	public void dayStatistic(Date date) {
		String day = TimeUtil.getDay(date);
		logger.info("{}.终端统计开始...",day);
		//先统计应用的留存用户和新增用户
		Long[] size = statisticDayUserData(day);
		Datastore ds = MongoDBDataStore.getStatistic();
		DeviceDayStatistic res = new DeviceDayStatistic();
		res.setType(Constant.REPORT_TYPE_TERMINAL);
		res.setDay(TimeUtil.getDay(date));
		res.setField("留存用户");
		res.setCount(size[0]);
		ds.save(res);
		
		DeviceDayStatistic new_u = new DeviceDayStatistic();
		new_u.setType(Constant.REPORT_TYPE_TERMINAL);
		new_u.setDay(TimeUtil.getDay(date));
		new_u.setField("新增用户");
		new_u.setCount(size[1]);
		ds.save(new_u);
		
		//统计品牌
		executeDayMapReduce(day, Constant.REPORT_TYPE_BRAND);
		executeDayMapReduce(day, Constant.REPORT_TYPE_MODEL);
		executeDayMapReduce(day, Constant.REPORT_TYPE_OS);
		executeDayMapReduce(day, Constant.REPORT_TYPE_LOCATION);
		executeDayMapReduce(day, Constant.REPORT_TYPE_NET);
	    logger.info("{}.终端统计完成...",day);
	}
	
	@Override
	public DeviceDayStatistic queryLastData(String day) {
		Datastore ds = MongoDBDataStore.getStatistic();
		return  ds.find(DeviceDayStatistic.class,"day",day).get();
	}
	
	public List<DeviceDayStatistic> queryDayStatistic(Date date)
	{
		Datastore ds = MongoDBDataStore.getStatistic();
		return ds.find(DeviceDayStatistic.class, "day", TimeUtil.getDay(date)).asList();
	}
	
	public static void main(String[] args) {
		long s = System.currentTimeMillis();
		AppImeiServiceImpl impl = new AppImeiServiceImpl();
		List<String> arrs = impl.writeImeiData();
		generate(arrs);
		System.out.println(System.currentTimeMillis() - s);
	}
}
	
