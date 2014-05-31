package com.jukuad.statistic.config;

import java.net.UnknownHostException;

import org.mongodb.morphia.AdvancedDatastore;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import com.jukuad.statistic.device.AdHourImei;
import com.jukuad.statistic.device.AppHourImei;
import com.jukuad.statistic.pojo.AdDayStatistic;
import com.jukuad.statistic.pojo.AdResult;
import com.jukuad.statistic.pojo.AppDayStatistic;
import com.jukuad.statistic.pojo.AppResult;
import com.jukuad.statistic.pojo.Click;
import com.jukuad.statistic.pojo.DaySum;
import com.jukuad.statistic.pojo.DeviceDayStatistic;
import com.jukuad.statistic.pojo.Download;
import com.jukuad.statistic.pojo.Imei;
import com.jukuad.statistic.pojo.Install;
import com.jukuad.statistic.pojo.Push;
import com.jukuad.statistic.pojo.Request;
import com.jukuad.statistic.pojo.View;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

/**
 * MongoDB联合Morphia框架生成DataStore的类
 * @author wally
 *
 */
public class MongoDBDataStore 
{
	private static MongoClient client = null;
	private static Datastore data = null;
	private static Datastore statistic = null;
	private static Datastore temp = null;
	
	private static AdvancedDatastore dataADS = null;
	private static AdvancedDatastore tempADS = null;
	
	public static MongoClient getClient()
	{
		if(client == null) 
		{
			MongoClientOptions mongoOptions = MongoClientOptions.builder().connectionsPerHost(1000).build();
			try {
				client = new MongoClient(new ServerAddress(MongoDBConnConfig.SERVER, MongoDBConnConfig.PORT),mongoOptions);
			} catch (UnknownHostException e) {
				return null;
			}
		}
		return client;
	}
	
	public static Datastore getData()
	{
		if(data == null)
		{
			Morphia morphia= new Morphia();
			MongoClient client = getClient();
			morphia.map(Request.class)
	           .map(Push.class)
	           .map(Click.class)
	           .map(View.class)
	           .map(Download.class)
	           .map(Install.class)
		       .map(Imei.class)
		       .map(AppHourImei.class)
		       .map(AdHourImei.class);
			data = morphia.createDatastore(client, MongoDBConnConfig.DATABASE_DATA);
			data.ensureIndexes();
			data.getCollection(Imei.class).createIndex(new BasicDBObject("time",1));
		}
		return data;
	}
	
	public static AdvancedDatastore getDataADS()
	{
		if(dataADS == null)
		{
			dataADS = (AdvancedDatastore) data;
		}
		return dataADS;
	}
	
	public static Datastore getStatistic()
	{
		if(statistic == null)
		{
			Morphia morphia= new Morphia();
			MongoClient client = getClient();
			morphia.map(AdDayStatistic.class)
	           .map(AppDayStatistic.class)
	           .map(AppResult.class)
	           .map(AdResult.class)
	           .map(DaySum.class)
	           .map(DeviceDayStatistic.class);
			statistic = morphia.createDatastore(client, MongoDBConnConfig.DATABASE_STATISTIC);
			statistic.ensureIndexes();
		}
		return statistic;
	}
	
	public static Datastore getTemp()
	{
		if(temp == null)
		{
			Morphia morphia= new Morphia();
			MongoClient client = getClient();
			morphia.map(Request.class)
	           .map(Push.class)
	           .map(Click.class)
	           .map(View.class)
	           .map(Download.class)
	           .map(Install.class);
//	           .map(Test.class);
			temp = morphia.createDatastore(client, MongoDBConnConfig.DATABASE_TEMP);
		}
		return temp;
	}
	
	public static AdvancedDatastore getTempADS()
	{
		if(tempADS == null)
		{
			tempADS = (AdvancedDatastore) temp;
		}
		return tempADS;
	}
}