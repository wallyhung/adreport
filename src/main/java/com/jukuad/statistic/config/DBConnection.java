package com.jukuad.statistic.config;

import java.io.IOException;
import java.sql.SQLException;

import org.ne81.commons.db.SqlPool;

public class DBConnection 
{
	private static SqlPool pool = null;
	
	public static synchronized SqlPool getInstance() throws SQLException, IOException
	{
		if(pool == null)
		{
			pool = new SqlPool("jdbc.properties", "/logs/adstatistic/insert.log");
		}
		return pool;
	}
}
