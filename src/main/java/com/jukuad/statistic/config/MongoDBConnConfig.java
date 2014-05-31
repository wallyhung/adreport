package com.jukuad.statistic.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBConnConfig
{
	private final static Logger logger = LoggerFactory.getLogger(MongoDBConnConfig.class);
	//MongoDB主机名称或IP
    public static String SERVER =              "127.0.0.1";
    //MongoDB端口
    public static int PORT =                    27017;
    public static String USER =                 "root";
    public static String PASS =                 "root";
    
    /***数据库实例名称**/
	public  static String DATABASE_TEMP =      "temp";
	public  static String DATABASE_DATA =      "data";
	public  static String DATABASE_STATISTIC = "statistic";
	
	static
	{
		Properties properties = new Properties();
		InputStream in = null;
		try
		{
			in = Thread.currentThread().getContextClassLoader().getResourceAsStream("jdbc.properties");
			properties.load(in);
			
			SERVER = 			 properties.getProperty("server") == null || "".equals(properties.getProperty("server") == null) ? 
				     			 "localhost" : properties.getProperty("server");
			PORT =               properties.getProperty("port") == null || "".equals(properties.getProperty("port") == null) ? 
			                     27017 : Integer.parseInt(properties.getProperty("port"));
			DATABASE_TEMP =      properties.getProperty("temp") == null || "".equals(properties.getProperty("temp") == null) ? 
				                 "temp" : properties.getProperty("temp");
			DATABASE_DATA      = properties.getProperty("data") == null || "".equals(properties.getProperty("data") == null) ? 
				                 "data" : properties.getProperty("data");
			DATABASE_STATISTIC = properties.getProperty("statistic") == null || "".equals(properties.getProperty("statistic") == null) ? 
				                 "statistic" : properties.getProperty("statistic");
		}catch(Exception e)
		{
			logger.error("mongodb config error:{}",e.getMessage());
		}finally{
			try {
				if(in != null){
					in.close();
				}
			} catch (IOException e) 
			{
				logger.error("inputstream in close fail:{}",e.getMessage());
			}
		}
	}
	
}
