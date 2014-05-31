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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.pojo.Imei;
import com.jukuad.statistic.util.ObjectUtil;
import com.jukuad.statistic.util.TimeUtil;

public class Temp {
	private static final Logger logger = LoggerFactory.getLogger(Temp.class);
	private static String path = "d:/bin/logs/push/";
	private static Map<String, Long> map = new HashMap<String, Long>();
	public static List<AdFeedback> parse(String hour)
	{
		String relpath = path + hour + ".log";
		//创建Jackson全局的objectMapper 它既可以用于序列化 也可以用于反序列化
		ObjectMapper objectMapper = new ObjectMapper();
	    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		//得到JSON处理的工厂对象
		JsonFactory jsonFactory= objectMapper.getFactory();
		
		//进入读文件阶段
		InputStreamReader in = null;
		Integer idx = 1;
		List<AdFeedback> list = new ArrayList<AdFeedback>();
		try 
		{
			in = new InputStreamReader(new FileInputStream(new File(relpath)), "UTF-8");
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
						AdFeedback object = jsonParser.readValueAs(AdFeedback.class);
						if (object != null){
							list.add(object);
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
	
	public static void main(String[] args) {
		
		String hour1 = "2014-05-17-15";
		String hour2 = "2014-05-17-06";
		List<String> timeArrs = TimeUtil.getDistanceTimeHourArray(hour1, hour2);
		for (String hour : timeArrs) {
			List<AdFeedback> list = parse(hour);
			Imei entity = null;
			for (AdFeedback message : list) 
			{
				entity = ObjectUtil.pushToImei(message);
				if(entity.getProvince().indexOf("北京") > -1)
				{
					if(!map.containsKey(entity.getValue()))
					{
						map.put(entity.getValue(), (long) 1);
					}
				}
			}
			System.out.println("hour:" + hour + "完成...");
		}
		System.out.println(map.size());
		
		Set<String> keys = map.keySet();
		for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
			String key = iterator.next();
			System.err.println(key);
		}
	}

}
