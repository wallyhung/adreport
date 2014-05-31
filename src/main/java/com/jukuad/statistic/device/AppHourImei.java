package com.jukuad.statistic.device;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.jukuad.statistic.pojo.Imei;
import com.mongodb.DBObject;

@Entity(value="app_imei", noClassnameStored = true)
@Indexes(@Index(name="idx_app_imei", value="fid,-hour"))
public class AppHourImei extends Imei {
	
	private String hour;
	
	private DBObject imei;
	
	public AppHourImei() {
	}
	
	public AppHourImei(String imei) 
	{
		super(imei);
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public DBObject getImei() {
		return imei;
	}

	public void setImei(DBObject imei) {
		this.imei = imei;
	}

}
