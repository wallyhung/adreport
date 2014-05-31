package com.jukuad.statistic.device;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.jukuad.statistic.pojo.Imei;

@Entity(value="ad_imei", noClassnameStored = true)
@Indexes(@Index(name="idx_ad_imei", value="adid,-hour"))
public class AdHourImei extends Imei {
	
	private String hour;
	
	public AdHourImei() {
	}
	
	public AdHourImei(String imei) 
	{
		super(imei);
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

}
