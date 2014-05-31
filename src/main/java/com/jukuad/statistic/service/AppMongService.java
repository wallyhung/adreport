package com.jukuad.statistic.service;

import java.util.Date;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jukuad.statistic.config.MongoDBConnConfig;
import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.device.AdHourImei;
import com.jukuad.statistic.device.AppHourImei;
import com.jukuad.statistic.device.AppImeiServiceImpl;
import com.jukuad.statistic.device.ImeiService;
import com.jukuad.statistic.pojo.AdDayStatistic;
import com.jukuad.statistic.pojo.AdResult;
import com.jukuad.statistic.pojo.AppDayStatistic;
import com.jukuad.statistic.pojo.AppResult;
import com.jukuad.statistic.pojo.Attach;
import com.jukuad.statistic.pojo.Click;
import com.jukuad.statistic.pojo.DaySum;
import com.jukuad.statistic.pojo.DeviceDayStatistic;
import com.jukuad.statistic.pojo.Download;
import com.jukuad.statistic.pojo.Imei;
import com.jukuad.statistic.pojo.Install;
import com.jukuad.statistic.pojo.Push;
import com.jukuad.statistic.pojo.Request;
import com.jukuad.statistic.pojo.View;
import com.jukuad.statistic.util.TimeUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;

public class AppMongService<T> implements MongoService<T>
{
	private static final Logger logger = LoggerFactory.getLogger(AppMongService.class);
	private ImeiService service = new AppImeiServiceImpl();

	@Override
	public void executeMapReduce(Class<T> name, String database,String hour) 
	{
		String map = "function(){" 
	               + "emit(this.fid, {count : 1,time : this.time})"
				   + "}";
		
		String reduce = "function (key, values) {"
					  + "var res = {count : 0,time : ''};"
					  + "res.time = values[0].time;"
					  + "for (var i = 0; i < values.length; i++) {"
						    + "res.count += values[i].count; "
							+ "if (res.time > values[i].time) {"
							+ "res.time = values[i].time; "
							+"}"
						+"}"
						+"return res;"
					+"}";
		String outputCollection = "tmp";
		
	  //获取数据库一个实例
	  Datastore ds = MongoDBDataStore.getTemp();  
	  DBObject query = new BasicDBObject(); 
	  MapReduceOutput out = ds.getCollection(name).mapReduce(map, reduce, outputCollection, query);
	  //处理查询结果并入库
	  analyzeMapReduceResult(name,out,hour);
	}

	/**
	 * 处理请求数和展示数的map结果
	 * @param name
	 * @param out
	 */
	private void analyzeMapReduceResult(Class<T> name, MapReduceOutput out,String hour) 
	{
		DBCollection dc = out.getOutputCollection();
        DBCursor cursor = dc.find();
        //获取一个数据库实例
        Datastore statistic = MongoDBDataStore.getStatistic();
        statistic.ensureIndexes();
        logger.info("app:{}:mapreduce执行状况：{}",name,dc.getStats());
        
        while (cursor.hasNext()) 
        {
			DBObject dbObject = (DBObject) cursor.next();
			String id =  (String) dbObject.get("_id");
			DBObject valueOb = (DBObject) dbObject.get("value");
			String count = String.valueOf(valueOb.get("count"));
			if(Request.class.equals(name))
			{
				AppResult res = new AppResult();
				res.setFid(id);
				res.setRequest(Double.valueOf(count).longValue());
				res.setFirst((Long) valueOb.get("time"));
				res.setTimestamp(System.currentTimeMillis());
				res.setHour(hour);
				
				//处理imei信息
				Integer[] size = service.statisticHourUserData(id, hour);
				res.setRemain(size[0]);
				res.setNew_u(size[1]);
				
				Key<AppResult> key = statistic.save(res);
				logger.debug("分析请求数，并入库：{}",key.getId());
			}
			else
			{
				Query<AppResult> query = statistic.createQuery(AppResult.class).field("fid").equal(id).field("hour").equal(hour);
				UpdateOperations<AppResult> ops = null;
				int size = query.asList().size();  //查询不存在情况：有可能是上个小时的推送，今天才发生的后续行为
				if(size > 0)
				{
					ops = parseMapReduceValue(statistic, name, Double.valueOf(count).longValue());
					statistic.update(query, ops);
					logger.debug("分析{}数，并更新：{}",name,ops.toString());
				}
				else
				{
					AppResult res = new AppResult();
					res.setFid(id);
					res.setFirst((Long) valueOb.get("time"));
					res.setTimestamp(System.currentTimeMillis());
					res.setHour(hour);
					parseMapReduceValue(res, name, Double.valueOf(count).longValue());
					Key<AppResult> key = statistic.save(res);
					logger.debug("分析推送数或展示数，不存在，新建并入库：{}",key.getId());
				}
			}
		}
	}
	
	private UpdateOperations<AppResult> parseMapReduceValue(Datastore ds, Class<T> name, long longValue) 
	{
		UpdateOperations<AppResult> ops = null;
		if(Push.class.equals(name))       ops = ds.createUpdateOperations(AppResult.class).set("push", longValue);
		if(View.class.equals(name))       ops = ds.createUpdateOperations(AppResult.class).set("view", longValue);
		return ops;
	}
	
	private UpdateOperations<AppResult> parseMapReduceValue(Datastore ds,Class<T> name, Attach attach) 
	{
		UpdateOperations<AppResult> ops = null;
		if(Click.class.equals(name))      ops = ds.createUpdateOperations(AppResult.class).set("click", attach);
		if(Download.class.equals(name))   ops = ds.createUpdateOperations(AppResult.class).set("download", attach);
		if(Install.class.equals(name))    ops = ds.createUpdateOperations(AppResult.class).set("install", attach);
		return ops;
	}

	private void parseMapReduceValue(AppResult res, Class<T> name, long longValue) 
	{
		if(Push.class.equals(name)) res.setPush(longValue);
		if(View.class.equals(name)) res.setView(longValue);
	}
	
	private void parseMapReduceValue(AppResult res, Class<T> name, Attach attach) 
	{
		if(Click.class.equals(name)) res.setClick(attach);
		if(Download.class.equals(name)) res.setDownload(attach);
		if(Install.class.equals(name)) res.setInstall(attach);
	}
	
	
	@Override
	public void executeAppByAdTypeMapReduce(Class<T> name, String database,String hour) 
	{
		String map = "function(){" +
						  "var key = this.fid;"+
						  "var value = {count:1,wall:0,oth:0,time:this.time};"+
					      "if(this.adid == this.appid) value.oth = 1;"+
						  "else value.wall = 1;"+
						  "emit(key,value);"+
					  "}";
		
		String reduce = "function (key, values) {"+
							"var res = {count:0,wall:0,oth:0,time:0};"+
				            "res.time = values[0].time;"+
							"values.forEach(function(val) {"+
								"res.count   += val.count;"+
								"res.wall += val.wall;"+
								"res.oth += val.oth;"+
								"if(val.time < res.time) res.time = val.time;"+
							"});"+
						    "res.on_reduce = 1;"+
							"return res;"+	
						"}";
		
		if(Click.class.equals(name))
		{
			map = "function(){" +
					  "var key = this.fid;"+
					  "var value = {count:1,cpa:0,cpc:0,time:this.time};"+
					  "if(this.type==1) value.cpa = 1;"+
					  "if(this.type==2) value.cpc = 1;"+
					  "emit(key,value);"+
				  "}";
			
			reduce = "function (key, values) {"+
						"var res = {count:0,cpa:0,cpc:0,time:''};"+
					    "res.time = values[0].time;"+
						"values.forEach(function(val) {"+
							"res.count += val.count;"+
							"res.cpa   += val.cpa;"+ 
							"res.cpc   += val.cpc;"+
							"if(val.time < res.time) res.time = val.time;"+
						"});"+
					    "res.on_reduce = 1;"+
						"return res;"+	
					"}";
		}
	   String outputCollection = "tmp";
	   //获取数据库一个实例
	   Datastore ds = MongoDBDataStore.getTemp();  
	   DBObject query = new BasicDBObject(); 
	   MapReduceOutput out = ds.getCollection(name).mapReduce(map, reduce, outputCollection, query);
	   //处理查询结果并入库
	   analyzeAppMapReduceResult(name,out,hour);
	}
	
	/**
	 * 处理app点击、下载、安装的数据
	 * @param name
	 * @param out
	 */
	private void analyzeAppMapReduceResult(Class<T> name, MapReduceOutput out,String hour) 
	{
		Datastore ds = MongoDBDataStore.getStatistic(); 
		DBCollection dc = out.getOutputCollection();
		logger.info("根据广告的类型，统计应用的数据,{}。mapreduce执行状况：{}",name,dc.getStats());
        DBCursor cursor = dc.find();
        while (cursor.hasNext()) 
        {
			DBObject dbObject = (DBObject) cursor.next();
			String id = (String) dbObject.get("_id");
			DBObject value = (DBObject) dbObject.get("value");
			Long time = (Long) value.get("time");
			Query<AppResult> query = ds.createQuery(AppResult.class).field("fid").equal(id).field("hour").equal(hour);
			UpdateOperations<AppResult> ops = null;
			if(Click.class.equals(name))
			{
				String cpa = String.valueOf(value.get("cpa"));
				String cpc = String.valueOf(value.get("cpc"));
				Attach attach = new Attach(Double.valueOf(cpc).longValue(), Double.valueOf(cpa).longValue(),0);
				int size = query.asList().size();  //查询不存在情况：有可能是上个小时的推送，今天才发生的后续行为
				if(size > 0)
				{
					ops = parseMapReduceValue(ds, name, attach);
					ds.update(query, ops);
					logger.debug("分析点击数，并更新：{}",name,ops.toString());
				}
				else
				{
					AppResult res = new AppResult();
					res.setFid(id);
					res.setTimestamp(System.currentTimeMillis());
					res.setHour(hour);
					res.setFirst(time);
					parseMapReduceValue(res, name, attach);
					Key<AppResult> key = ds.save(res);
					logger.debug("分析点击数，不存在，新建并入库：{}",key.getId());
				}
			}
			else
			{
				//包含下载和安装
				String wall = String.valueOf(value.get("wall"));
				String oth = String.valueOf(value.get("oth"));
				Attach attach = new Attach(Double.valueOf(wall).longValue(), Double.valueOf(oth).longValue());
				int size = query.asList().size();  //查询不存在情况：有可能是上个小时的推送，今天才发生的后续行为
				if(size > 0)
				{
					ops = parseMapReduceValue(ds, name, attach);
					ds.update(query, ops);
					logger.debug("分析下载或安装数，并更新：{}",name,ops.toString());
				}
				else
				{
					AppResult res = new AppResult();
					res.setFid(id);
					res.setTimestamp(System.currentTimeMillis());
					res.setHour(hour);
					res.setFirst(time);
					parseMapReduceValue(res, name, attach);
					Key<AppResult> key = ds.save(res);
					logger.debug("分析下载或安装数，不存在，新建并入库：{}",key.getId());
				}
			}
		}
	}
	
	@Override
	public void executeTempMapReduce(Date date) 
	{
		String map = "function(){" 
	                   + " var key = this.fid;"
	                   + " var value = {count: 1, request: this.request, push:this.push, view: this.view, c_cpc: 0, c_cpa: 0,d_wall: 0, d_oth: 0, i_wall: 0, i_oth: 0, new_u:this.new_u, first: this.first};"
	                   + " if(this.click != null){"
	                      + "value.c_cpc = this.click.cpc;"
	                      + "value.c_cpa = this.click.wall;"
	                   + "}"
	                   + " if(this.download != null){"
	                      + "value.d_wall = this.download.wall;"
	                      + "value.d_oth = this.download.oth;"
	                   + "}"
	                   + " if(this.install != null){"
	                      + "value.i_wall = this.install.wall;"
	                      + "value.i_oth = this.install.oth;"
	                   + "}"
	                   + "emit(key,value);"
				   + "}";
		
		String reduce = "function (key, values) {"
				  + "var res = {count:0, request:0, push:0, view:0, c_cpc:0, c_cpa:0, d_wall:0, d_oth:0, i_wall:0, i_oth:0, new_u:0, first:''};"
				  + "res.first = values[0].first;"	
				  + "for (var i = 0; i < values.length; i++) {"	
					  + "res.count += values[i].count; "		     
					  + "res.request += values[i].request;"	
					  + "res.push += values[i].push;"	
					  + "res.view += values[i].view;"	
					  + "res.c_cpc += values[i].c_cpc;"
					  + "res.c_cpa += values[i].c_cpa;"		     
					  + "res.d_wall += values[i].d_wall;"	
					  + "res.d_oth  += values[i].d_oth;"	
					  + "res.i_wall += values[i].i_wall;"	
					  + "res.i_oth  += values[i].i_oth;"
					  + "res.new_u  += values[i].new_u;"
					  + "if (res.first > values[i].first){"
							+ "res.first = values[i].first; "	
					  + "}"
					  + "}"
				  + "return res;"
				+"}";
		String outputCollection = "tmp";
		
		 Datastore ds = MongoDBDataStore.getStatistic();
	     BasicDBObject query = new BasicDBObject(); 
	     //此处特别注意：不要使用put方法，否则查询无效，比如：new BasicDBObject().put("$gt", updateTime)  { "hour" : { "$regex" : "2014-04-04"}}
//	     time.append("$gte", TimeUtil.getDayStart(date).getTime()).append("$lt", TimeUtil.getDayEnd(date).getTime());
//	     query.append("time", time);
	     query.append("hour", new BasicDBObject("$regex", TimeUtil.getDay(date)));
	     MapReduceOutput out = ds.getCollection(AppResult.class).mapReduce(map, reduce, outputCollection, query);
	     //处理查询结果并入库
	     analyzeTempMapReduceResult(out,date);
	}

	/**
	 * 对一天的日志进行map操作
	 * 保存数据到日统计文档中
	 * 
	 * 注意：mapreduce的结果，如果临时的分组结果条数，只为1将不会进行reduce操作
	 * 得到的结果数请求数为long，转化保存，此处统一准话为string，再转成long
	 * 
	 * @param out
	 * @param date
	 */
	private void analyzeTempMapReduceResult(MapReduceOutput out, Date date) 
	{
		DBCollection dc = out.getOutputCollection();
        DBCursor cursor = dc.find();
        //获取一个数据库实例
        Datastore statistic = MongoDBDataStore.getStatistic();
        logger.info("{}：一天应用数据mapreduce执行状况：{}....",TimeUtil.getDay(date),dc.getStats());
        
        while (cursor.hasNext()) 
        {
			DBObject dbObject = (DBObject) cursor.next();
			AppDayStatistic res = new AppDayStatistic();
			
			DBObject valueOb = (DBObject) dbObject.get("value");
			String request = String.valueOf(valueOb.get("request"));
			String push = String.valueOf(valueOb.get("push"));
			String view = String.valueOf(valueOb.get("view"));
			
			String click_cpc = String.valueOf(valueOb.get("c_cpc"));
			String click_cpa = String.valueOf(valueOb.get("c_cpa"));
			
			String down_wall = String.valueOf(valueOb.get("d_wall"));
			String down_oth = String.valueOf(valueOb.get("d_oth"));
				
			String ins_wall = String.valueOf(valueOb.get("i_wall"));
			String ins_oth = String.valueOf(valueOb.get("i_oth"));
			
			String new_u = String.valueOf(valueOb.get("new_u"));
			
			Attach click_Attach = new Attach(Double.valueOf(click_cpc).longValue(), Double.valueOf(click_cpa).longValue(), 0);
			Attach down_Attach = new Attach(Double.valueOf(down_wall).longValue(), Double.valueOf(down_oth).longValue());
			Attach ins_Attach = new Attach(Double.valueOf(ins_wall).longValue(), Double.valueOf(ins_oth).longValue());
				
			res.setClick(click_Attach);
			res.setDownload(down_Attach);
			res.setInstall(ins_Attach);
			
			String id = (String) dbObject.get("_id");
			res.setFid(id);
			
			res.setRequest(Double.valueOf(request).longValue());
			res.setPush(Double.valueOf(push).longValue());
			res.setView(Double.valueOf(view).longValue());
			
			res.setDay(TimeUtil.getDay(date));
			
			long first = (Long) valueOb.get("first");
			//查询第一次访问时间
			AppDayStatistic app = queryFirstRequestApp(MongoDBConnConfig.DATABASE_STATISTIC, res.getFid());
			if(app != null && app.getTimestamp() < first)
				res.setTimestamp(app.getTimestamp());
			else res.setTimestamp(first);
			
			//获取用户的留存、新增
			long remain = service.statisticRemainUserData(id, TimeUtil.getDay(date));
			res.setRemain(remain);
			res.setNew_u(Double.valueOf(new_u).longValue());
			
			Key<AppDayStatistic> key = statistic.save(res);
			logger.debug("{}:{}分析临时数据汇总，并入库：{}","AppDayStatistic",TimeUtil.getDay(date),key.getId());
        }
	}
	
	
	
	/**
	 * 查询app第一次请求的时间
	 * @param database
	 * @param fid
	 * @return
	 */
	private AppDayStatistic queryFirstRequestApp(String database,String fid)
	{
		AppDayStatistic first = null;
		Datastore ds = MongoDBDataStore.getStatistic(); 
		Query<AppDayStatistic> query = ds.createQuery(AppDayStatistic.class).field("fid").equal(fid).order("time").limit(1);
		List<AppDayStatistic> res = query.asList();
		if(res.size() > 0 && res!= null) first = res.get(0);
		return first;
	}
	
	
	@Override
	public void executeDayMapReduce(Class<T> name, String database,Date date) {
	}
	
	@Override
	public long queryNewAppsCount(String database, Date date) {
		return 0;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public List queryDayStatistic(Date date) 
	{
		Datastore ds = MongoDBDataStore.getStatistic();
		Query<AppDayStatistic> query = ds.createQuery(AppDayStatistic.class);
		query.field("day").equal(TimeUtil.getDay(date));
		return query.asList();
	}
	
	@Override
	public List<T> queryHourStatistic(Class<T> name, String hour) 
	{
		Datastore ds = MongoDBDataStore.getStatistic();
		Query<T> query = ds.createQuery(name);
		query.field("hour").equal(hour);
		return query.asList();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void fillInWebAppData(String database, Date date) 
	{
		long c_sum = 0;
		long i_sum = 0;
		List<AppDayStatistic> list = queryDayStatistic(date);
		for (AppDayStatistic app : list) 
		{
			if(app.getClick() != null) c_sum += app.getClick().getCpc();
			if(app.getInstall() != null) i_sum += app.getInstall().getOth() + app.getInstall().getWall();
		}
		
		//修改
		Datastore ds = MongoDBDataStore.getStatistic();
		Query<AppDayStatistic> query = ds.createQuery(AppDayStatistic.class).field("day").equal(TimeUtil.getDay(date));
		UpdateOperations<AppDayStatistic> ops = ds.createUpdateOperations(AppDayStatistic.class).set("c_sum", c_sum).set("i_sum", i_sum);
		ds.update(query, ops);
	}
	
	public boolean validateAppDayStatistic(String day) {
		Datastore ds = MongoDBDataStore.getStatistic();
		Query<AppDayStatistic> query = ds.createQuery(AppDayStatistic.class).filter("day", day);
		List<AppDayStatistic> applist = query.asList();
		if(applist.size() == 0) return true;
		//只需要判断一个就知道是否日统计完成
		if(applist.get(0).getI_sum() == 0) return true;
		return false;
	}
	
	@Override
	public boolean validateDayStatistic(String day) {
		Datastore ds = MongoDBDataStore.getStatistic();
		DaySum sum = ds.find(DaySum.class, "day", day).get();
		if(sum == null) return true;
		return false;
	}
	
	@Override
	public void daleteHourData(String hour) {
//		long start = TimeUtil.getDayFromHourString(hour).getTime();
		Datastore ds = MongoDBDataStore.getData();
		//删掉后
//		Query<Request> request = ds.createQuery(Request.class).field("time").greaterThanOrEq(start);
//		ds.delete(request);
//		Query<Push> push = ds.createQuery(Push.class).field("time").greaterThanOrEq(start);
//		ds.delete(push);
//		
//		Query<View> view = ds.createQuery(View.class).field("time").greaterThanOrEq(start);
//		ds.delete(view);
//		
//		Query<Click> click = ds.createQuery(Click.class).field("time").greaterThanOrEq(start);
//		ds.delete(click);
//		
//		Query<Install> install = ds.createQuery(Install.class).field("time").greaterThanOrEq(start);
//		ds.delete(install);
//		
//		Query<Download> down = ds.createQuery(Download.class).field("time").greaterThanOrEq(start);
//		ds.delete(down);
		
		Query<AppHourImei> appimei = ds.createQuery(AppHourImei.class).field("hour").equal(hour);
		ds.delete(appimei);
		
		Query<AdHourImei> adimei = ds.createQuery(AdHourImei.class).field("hour").equal(hour);
		ds.delete(adimei);
		
		//删掉统计数据
		Datastore statis = MongoDBDataStore.getStatistic();
		Query<AppResult> app = statis.createQuery(AppResult.class).field("hour").equal(hour);
		statis.delete(app);
		
		Query<AdResult> ad = statis.createQuery(AdResult.class).field("hour").equal(hour);
		statis.delete(ad);
	}
	
	
	@Override
	public void daleteDayData(String day) {
		Datastore statis = MongoDBDataStore.getStatistic();
		Query<AppDayStatistic> app = statis.createQuery(AppDayStatistic.class).field("day").equal(day);
		statis.delete(app);
		
		Query<AdDayStatistic> ad = statis.createQuery(AdDayStatistic.class).field("day").equal(day);
		statis.delete(ad);
		
		Query<DeviceDayStatistic> device = statis.createQuery(DeviceDayStatistic.class).field("day").equal(day);
		statis.delete(device);
		
		Query<DaySum> sum = statis.createQuery(DaySum.class).field("day").equal(day);
		statis.delete(sum);
	}
	@Override
	public boolean existNewHourData(Class<T> name, String hour) {
		Datastore temp = MongoDBDataStore.getTemp();
		long start = TimeUtil.getDayFromHourString(hour).getTime();
		long end = TimeUtil.getDayNextHourDate(hour).getTime();
		Query<T> query = temp.createQuery(name).field("time").greaterThanOrEq(start).field("time").lessThan(end);
		long size = query.countAll();
		if(size == 0) return false;
		return true;
	}
	
	@Override
	public void deleteDayBackup(Date date) {
		//删除7天以前的数据
		Date seven = TimeUtil.getSevenDaysBefore(date);
		long start = seven.getTime();
		Datastore ds = MongoDBDataStore.getData();
		//删掉后
		Query<Request> request = ds.createQuery(Request.class).field("time").lessThan(start);
		ds.delete(request);
		Query<Push> push = ds.createQuery(Push.class).field("time").lessThan(start);
		ds.delete(push);
		
		Query<View> view = ds.createQuery(View.class).field("time").lessThan(start);
		ds.delete(view);
		
		Query<Click> click = ds.createQuery(Click.class).field("time").lessThan(start);
		ds.delete(click);
		
		Query<Install> install = ds.createQuery(Install.class).field("time").lessThan(start);
		ds.delete(install);
		
		Query<Download> down = ds.createQuery(Download.class).field("time").lessThan(start);
		ds.delete(down);
		
		Query<Imei> imei = ds.createQuery(Imei.class).field("time").lessThan(start);
		ds.delete(imei);
		
		//删除终端imei信息
		Date sevenBegin = TimeUtil.getDayStart(seven);
		String hour = TimeUtil.getDayHour(sevenBegin);
		Query<AppHourImei> appimei = ds.createQuery(AppHourImei.class).field("hour").lessThan(hour);
		ds.delete(appimei);
		Query<AdHourImei> adimei = ds.createQuery(AdHourImei.class).field("hour").lessThan(hour);
		ds.delete(adimei);
	}
}
