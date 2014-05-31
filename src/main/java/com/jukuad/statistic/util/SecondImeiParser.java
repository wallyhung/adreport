package com.jukuad.statistic.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jukuad.statistic.config.MongoDBDataStore;
import com.jukuad.statistic.device.ImeiCache;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.pojo.Imei;
import com.mongodb.DBObject;
public class SecondImeiParser
{
	private static final Logger logger = LoggerFactory.getLogger(SecondImeiParser.class);
	private ImeiCache cache = ImeiCache.getInstance();
	//日志文件路径
	private String path;
	//该小时的imei缓存，防止本小时的缓存
	private Map<String, Integer> imeiMap = new HashMap<String, Integer>();
	public SecondImeiParser() {
	}
	
	public SecondImeiParser(String path)
	{
		this.path = path;
	}
	
	public List<DBObject> parse()
	{
		String hour = path.substring(path.lastIndexOf("/")+1,path.length()-4);
		//创建Jackson全局的objectMapper 它既可以用于序列化 也可以用于反序列化
		ObjectMapper objectMapper = new ObjectMapper();
	    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		//得到JSON处理的工厂对象
		JsonFactory jsonFactory= objectMapper.getFactory();
		
		//进入读文件阶段
		InputStreamReader in = null;
		Integer idx = 1;
		int count = 1;
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
						ClientMessage object = jsonParser.readValueAs(ClientMessage.class);
						if(object != null)
						{
							//判断是否在小时缓存
							if(!imeiMap.containsKey(object.getImei()))
							{
								//判断是否已经入库
								if(!cache.hasImei(object.getImei(),hour)){
									DBObject o = ObjectUtil.clientMessToImeiObj(object);
									list.add(o);
								}
								imeiMap.put(object.getImei(), count);
								count++;
							}
						}
						
					} catch (Exception e) 
					{
						logger.error("{}：设备信息解析数据错误在第{}行，具体的内容为：{}",idx,currentJsonStr);
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
	
	public void save() 
	{
		//得到Morphia框架的Datastore对象用于数据库操作
		Datastore ds = MongoDBDataStore.getData();
		List<DBObject> list = this.parse();
		ds.getCollection(Imei.class).insert(list);
		//同步缓存信息
		cache.syncImeiCache(list);
	}
}
