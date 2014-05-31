package com.jukuad.statistic.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.log.SoftFeedback;
import com.jukuad.statistic.pojo.Click;
import com.jukuad.statistic.pojo.Download;
import com.jukuad.statistic.pojo.Install;
import com.jukuad.statistic.pojo.Push;
import com.jukuad.statistic.pojo.Request;
import com.jukuad.statistic.pojo.View;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
public class FileParser<T> implements Runnable
{
	
	private static final Logger logger = LoggerFactory.getLogger(FileParser.class);
	//解析的文件对应的pojo
	private Class<T> className;
	//日志文件路径
	private String path;
	private int type;
	
	//当前线程运行的任务数
	private CountDownLatch count;
	
	public FileParser() {
	}
	
	public FileParser(int type,Class<T> className,String path,CountDownLatch count)
	{
		this.type = type;
		this.path = path;
		this.className = className;
		this.count = count;
	}
	
	private List<DBObject> parse()
	{
		//创建Jackson全局的objectMapper 它既可以用于序列化 也可以用于反序列化
		ObjectMapper objectMapper = new ObjectMapper();
	    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		//得到JSON处理的工厂对象
		JsonFactory jsonFactory= objectMapper.getFactory();
		//进入读文件阶段
		InputStreamReader in = null;
		Integer idx = 1;
		List<DBObject> list = new ArrayList<DBObject>();
		try 
		{
			in = new InputStreamReader(new FileInputStream(new File(path)), "UTF-8");
			BufferedReader br = new BufferedReader(in);
			String currentJsonStr= null;
			try {
				//按行读取
				while((currentJsonStr = br.readLine()) != null){
					if(currentJsonStr.trim().equals("")) continue;
					//进入反序列化阶段
					//通过JSON处理工厂对象创建JSON分析器
					JsonParser jsonParser= jsonFactory.createParser(currentJsonStr);
					try {
						//反序列化的关键
						T object = jsonParser.readValueAs(className);
						if (object != null){
							DBObject entity = parseDBObject(object);
							list.add(entity);
						}
						
					} catch (Exception e) 
					{
						logger.error("{}：日志解析数据错误在第{}行，具体的内容为：{}",className,idx,currentJsonStr);
						continue;
					}
					
					idx++;
				}
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
			finally{
				if (br != null) {
	                try {
	                    br.close();
	                } catch (IOException e1) {
	                	logger.error("关闭读取文件的缓冲流出错：{}。",e1.getMessage());
	                }
	            }
				if (in != null) {
	                try {
	                    in.close();
	                } catch (IOException e2) {
	                	logger.error("关闭读取文件的缓冲流出错：{}。",e2.getMessage());
	                }
	            }
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (UnsupportedEncodingException e3) {
			logger.error(e3.getMessage());
		} 
		return list;
	}
	
	private DBObject parseDBObject(T t)
	{
		switch (type) {
		case 1:
			return ObjectUtil.clientMessToReqObj((ClientMessage)t);
		case 2:
			return ObjectUtil.adFeedbackToPushObj((AdFeedback)t);
		case 3:
			return ObjectUtil.adFeedbackToPushObj((AdFeedback)t);
		case 4:
			return ObjectUtil.adFeedbackToClickObj((AdFeedback)t);
		case 5:
			return ObjectUtil.softFeedbackToDownObj((SoftFeedback)t);
		case 6:
			return ObjectUtil.softFeedbackToDownObj((SoftFeedback)t);
		default:
			return null;
		}
	}
	
	private Class<?> parseCollectionName()
	{
		switch (type) {
		case 1:
			return Request.class;
		case 2:
			return Push.class;
		case 3:
			return View.class;
		case 4:
			return Click.class;
		case 5:
			return Download.class;
		case 6:
			return Install.class;
		default:
			return null;
		}
	}
	
	@Override
	public void run() 
	{
		//得到Morphia框架的Datastore对象用于数据库操作
		List<DBObject> list = this.parse();
		Datastore temp = MongoDBDataStore.getTemp();
		Datastore data = MongoDBDataStore.getData();
		
		temp.getCollection(parseCollectionName()).insert(list);
		data.getCollection(parseCollectionName()).insert(list);
		switch (type) {
		case 1:
			temp.getCollection(Request.class).createIndex(new BasicDBObject("fid",1));
			break;
		case 2:
			temp.getCollection(Push.class).createIndex(new BasicDBObject("adid",1));
			break;
		default:
			break;
		}
		count.countDown();
		logger.info("日志分析任务完成：{}，当前线程运行的任务数为{}。",className,count.getCount());
	}
}
