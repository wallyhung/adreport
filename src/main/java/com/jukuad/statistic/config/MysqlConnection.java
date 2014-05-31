package com.jukuad.statistic.config;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MysqlConnection 
{
	private final static Logger logger = LoggerFactory.getLogger(MysqlConnection.class);
	private static Connection connection = null;
	
	public static String driver;
	public static String url;
	public static String usr;
	public static String psw;
	public static String logName;
	
	public static Connection getConnection()
	{
		if(connection != null) return connection;
		else
		{
			Properties properties = new Properties();
			InputStream in = null;
			try
			{
				in = Thread.currentThread().getContextClassLoader().getResourceAsStream("jdbc.properties");
				properties.load(in);
				Class.forName(properties.getProperty("driver"));
				connection = DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"));
			}catch(Exception e)
			{
				logger.error("database config error:{}",e.getMessage());
			}finally{
				try {
					if(in != null){
						in.close();
					}
				} catch (IOException e) 
				{
					logger.error("ioexception in close fail:{}",e.getMessage());
				}
			}
			return connection;
		}
	}
	
	
	//关闭连接
	public static void close(Connection connection){
		try {
			if(connection != null){
				connection.close();
			}
		} catch (SQLException e) {
			logger.error("inputstream in close fail:{}",e.getMessage());
		}
	}
	//关闭连接
	public static void CloseAll(ResultSet rs, Statement st,Connection connection){
		if(rs != null){
			try {
				rs.close();
			} catch (SQLException e) {
			}
		}
		if(st != null){
			try {
				st.close();
			} catch (SQLException e) {
			}
		}
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}
}
