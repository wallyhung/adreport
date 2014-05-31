package com.jukuad.statistic.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jukuad.statistic.log.ClientMessage;

public class TestParser<T> {
	
	private static final Logger logger = LoggerFactory.getLogger(TestParser.class);
	
	private String path = "D://bin//logs//request//2014-03-28-10.log";

	public ArrayList<T> parse(Class<T> className)
	{
		
		//创建Jackson全局的objectMapper 它既可以用于序列化 也可以用于反序列化
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
	    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		//得到JSON处理的工厂对象
		JsonFactory jsonFactory= objectMapper.getFactory();
		
		//进入读文件阶段
		FileReader fr = null;
		Integer idx = 1;
		List<T> list = new ArrayList<T>();
		try 
		{
			fr = new FileReader(new File(path));
			BufferedReader br = new BufferedReader(fr);
			String currentJsonStr= null;
			try {
				//按行读取
				while((currentJsonStr = br.readLine()) != null){
					currentJsonStr = new String(currentJsonStr.getBytes(), "UTF-8");
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
				if (fr != null) {
	                try {
	                    fr.close();
	                } catch (IOException e2) {
	                	logger.error("关闭读取文件的缓冲流出错：{}。",e2.getMessage());
	                }
	            }
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} 
		return (ArrayList<T>) list;
	}
	
	public static void main(String[] args) {
		
		TestParser<ClientMessage> parser = new TestParser<ClientMessage>();
		List<ClientMessage> list = parser.parse(ClientMessage.class);
		for (ClientMessage clientMessage : list) {
			System.out.println(clientMessage.getApp_name());
		}
		
	}
	
}
