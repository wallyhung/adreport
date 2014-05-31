package com.jukuad.statistic.service;

import java.util.Date;
import java.util.List;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jukuad.statistic.config.MongoDBConnConfig;
import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.pojo.AdResult;
import com.jukuad.statistic.pojo.DaySum;
import com.jukuad.statistic.pojo.Imei;
import com.jukuad.statistic.util.TimeUtil;

public class BaseServiceImpl<T> implements BaseService<T> 
{
	private static final Logger logger = LoggerFactory.getLogger(BaseServiceImpl.class);
	
	@SuppressWarnings("rawtypes")
	@Override
	public List queryDayStatistic(Date date) {
		return null;
	}
	
	@Override
	public DaySum queryDaySum(Date date) 
	{
		Datastore ds = MongoDBDataStore.getStatistic();
		return ds.find(DaySum.class, "day", TimeUtil.getDay(date)).get();
	}
	
	@Override
	public AdResult queryLastData(String database) 
	{
		AdResult last = null;
		Datastore ds = MongoDBDataStore.getStatistic();
		Query<AdResult> query = ds.find(AdResult.class).order("-time").limit(1);
		List<AdResult> res = query.asList();
		if(res.size() > 0) last = res.get(0);
		return last;
	}
	
	@Override
	public boolean validateStatisticLastHourLog(String database,long startLongTime) 
	{
		boolean bool = true;
		AdResult obj = this.queryLastData(database);
		if(obj != null)
		{
			if(obj.getTimestamp() > startLongTime) bool = false;
		}
		return bool;
	}
	
	@Override
	public boolean validateStatisticLastHourLog(String hour) {
		Datastore ds = MongoDBDataStore.getStatistic();
		Query<AdResult> query = ds.createQuery(AdResult.class).field("hour").equal(hour);
		for (AdResult adResult : query.asList()) {
			if(adResult.getInstall() > 0) return false;
		}
		return true;
	}
	
	
	
	@Override
	public List<T> queryTempStatisticList(Class<T> name,String database,Date date) 
	{
		List<T> res = null;
		Datastore ds = MongoDBDataStore.getStatistic();
		String time  = TimeUtil.getDay(date);
		Query<T> query = ds.createQuery(name);
		if(date != null)
		{
			logger.debug("广告每小时库查询数据，开始时间：{}，结束时间：{}。",TimeUtil.getDayStart(date).getTime(),TimeUtil.getDayEnd(date).getTime());
//			query.field("time").greaterThanOrEq(TimeUtil.getDayStart(date).getTime());
//			query.field("time").lessThan(TimeUtil.getDayEnd(date).getTime());
//			{ "hour" : { "$regex" : "2014-04-04"}}
			query.field("hour").contains(time);
			res = query.asList();
		}
		logger.debug("广告每小时库查询数据，结果数：{}。",res == null ? 0 : res.size());
		return res;
	}
	
	@Override
	public boolean isExistImei(String value) 
	{
		boolean bool = false;
		Datastore ds = MongoDBDataStore.getData();
		Imei imei= ds.find(Imei.class, "value", value).get();
		if(imei == null) bool = true;
		return bool;
	}
	
	
	public static void main(String[] args) {
		BaseService<AdResult> service = new BaseServiceImpl<AdResult>();
		AdResult result = service.queryLastData(MongoDBConnConfig.DATABASE_STATISTIC);
		System.out.println(result.getAdid() + "---------" + result.getPush() + "----------" + result.getTimestamp());
		
	}
}
