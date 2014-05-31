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

import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
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
public class Parser
{
	
	private static final Logger logger = LoggerFactory.getLogger(Parser.class);
	private static ObjectMapper objectMapper;
	private static JsonFactory jsonFactory;
	static{
		//创建Jackson全局的objectMapper 它既可以用于序列化 也可以用于反序列化
		objectMapper = new ObjectMapper();
	    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		//得到JSON处理的工厂对象
		jsonFactory= objectMapper.getFactory();
	}
	
	private static List<DBObject> parse(int type,String path)
	{
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
						DBObject object = parseDBObject(type,jsonParser);
						if (object != null){
							list.add(object);
						}
					} catch (Exception e) 
					{
						logger.error("{}：日志解析数据错误在第{}行，具体的内容为：{}",idx,currentJsonStr);
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
	
	
	
	private static DBObject parseDBObject(int type, JsonParser jsonParser) throws JsonProcessingException, IOException {
		switch (type) {
		case 1:
			ClientMessage req = jsonParser.readValueAs(ClientMessage.class);
			return ObjectUtil.clientMessToReqObj(req);
		case 2:
			AdFeedback push = jsonParser.readValueAs(AdFeedback.class);
			return ObjectUtil.adFeedbackToPushObj(push);
		case 3:
			AdFeedback view = jsonParser.readValueAs(AdFeedback.class);
			return ObjectUtil.adFeedbackToPushObj(view);
		case 4:
			AdFeedback click = jsonParser.readValueAs(AdFeedback.class);
			return ObjectUtil.adFeedbackToClickObj(click);
		case 5:
			SoftFeedback down = jsonParser.readValueAs(SoftFeedback.class);
			return ObjectUtil.softFeedbackToDownObj(down);
		case 6:
			SoftFeedback install = jsonParser.readValueAs(SoftFeedback.class);
			return ObjectUtil.softFeedbackToDownObj(install);
		default:
			return null;
		}
	}



	public static void insertBatch(int type,String path) 
	{
		//得到Morphia框架的Datastore对象用于数据库操作
		List<DBObject> list = parse(type, path);
		Datastore ds = MongoDBDataStore.getData();
		Datastore temp = MongoDBDataStore.getTemp();
		switch (type) {
		case 1:
			ds.getCollection(Request.class).insert(list);
			temp.getCollection(Request.class).insert(list);
			temp.getCollection(Request.class).createIndex(new BasicDBObject("fid",1));
			break;
		case 2:
			ds.getCollection(Push.class).insert(list);
			temp.getCollection(Push.class).insert(list);
			temp.getCollection(Push.class).createIndex(new BasicDBObject("adid",1));
			break;
		case 3:
			ds.getCollection(View.class).insert(list);
			temp.getCollection(View.class).insert(list);
			break;
		case 4:
			ds.getCollection(Click.class).insert(list);
			temp.getCollection(Click.class).insert(list);
			break;
		case 5:
			ds.getCollection(Download.class).insert(list);
			temp.getCollection(Download.class).insert(list);
			break;
		case 6:
			ds.getCollection(Install.class).insert(list);
			temp.getCollection(Install.class).insert(list);
			break;
		default:
			break;
		}
		logger.info("{}日志分析任务完成：...",path);
	}
}
