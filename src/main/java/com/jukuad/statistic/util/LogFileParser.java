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
import org.mongodb.morphia.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.log.SoftFeedback;
import com.jukuad.statistic.pojo.BaseEntity;
public class LogFileParser<T> implements Runnable
{
	
	private static final Logger logger = LoggerFactory.getLogger(LogFileParser.class);
	//解析的文件对应的pojo
	private Class<T> className;
	
	//日志文件路径
	private String path;
		
	//当前线程运行的任务数
	private CountDownLatch count;
	
	public LogFileParser() {
	}
	
	public LogFileParser(Class<T> className,String path,CountDownLatch count)
	{
		this.path = path;
		this.className = className;
		this.count = count;
	}
	
	public ArrayList<T> parse()
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
		List<T> list = new ArrayList<T>();
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
							list.add(object);
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
		return (ArrayList<T>) list;
	}
	
	
	private BaseEntity parseDBObject(T t)
	{
		BaseEntity entity = null;
		if(path.indexOf(Constant.PATH_REQUEST) > -1)
			return ObjectUtil.clientMessToRequest((ClientMessage) t);
		if(path.indexOf(Constant.PATH_INFO) > -1)
			return ObjectUtil.clientMessToInfo((ClientMessage) t);
		if(path.indexOf(Constant.PATH_PUSH) > -1)
			return ObjectUtil.adFeedbackToPush((AdFeedback) t);
		if(path.indexOf(Constant.PATH_VIEW) > -1)
			return ObjectUtil.adFeedbackToView((AdFeedback) t);
		if(path.indexOf(Constant.PATH_CLICK) > -1)
			return ObjectUtil.adFeedbackToClick((AdFeedback) t);
		if(path.indexOf(Constant.PATH_DOWNLOAD) > -1)
			return ObjectUtil.softFeedbackToDown((SoftFeedback) t);
		if(path.indexOf(Constant.PATH_INSTALL) > -1)
			return ObjectUtil.softFeedbackToInstall((SoftFeedback) t);
		return entity;
	}
	
	
	
	@Override
	public void run() 
	{
		//得到Morphia框架的Datastore对象用于数据库操作
		Datastore ds = MongoDBDataStore.getTemp();
		Datastore back = MongoDBDataStore.getData();
		List<T> list = this.parse();
		for (T t : list) 
		{
			BaseEntity entity = parseDBObject(t);
			Key<BaseEntity> key = ds.save(entity);
			Key<BaseEntity> backkey = back.save(entity);
			if (key != null && backkey != null)
			{
			    logger.debug("已存入{}数据，keyid：{}。",className,key.getId());
			    entity = null;
			}else
			{
				logger.error("数据保存mongodb出错：{}。",t);
				break;
			}
		}
		count.countDown();
		logger.info("日志分析任务完成：{}，当前线程运行的任务数为{}。",className,count.getCount());
	}

}
