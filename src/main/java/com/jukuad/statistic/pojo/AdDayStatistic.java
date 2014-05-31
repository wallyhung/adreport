package com.jukuad.statistic.pojo;


import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.utils.IndexDirection;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * 每天每个应用的统计结果
 * @author Administrator
 *
 */
@Entity(value="ad_day", noClassnameStored = true)
public class AdDayStatistic extends BaseEntity
{
	 //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
	@Id
	//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
	@JsonIgnore(value= true)
	private ObjectId id;
	
    private long                request;
    private long                push;
    private long                view;
    private long                click;
    private long                download;
    private long                install;
    private long                alive; //终端数
    private String              adid;
    @Indexed(value=IndexDirection.DESC, name="idx_day_ad")
    private String                day;

    ///~ getter and setter
	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public long getRequest() {
		return request;
	}

	public void setRequest(long request) {
		this.request = request;
	}

	
	public long getPush() {
		return push;
	}

	public void setPush(long push) {
		this.push = push;
	}

	public long getView() {
		return view;
	}

	public void setView(long view) {
		this.view = view;
	}


	public long getClick() {
		return click;
	}

	public void setClick(long click) {
		this.click = click;
	}

	public long getDownload() {
		return download;
	}

	public void setDownload(long download) {
		this.download = download;
	}

	public long getInstall() {
		return install;
	}

	public void setInstall(long install) {
		this.install = install;
	}

	public long getAlive() {
		return alive;
	}

	public void setAlive(long alive) {
		this.alive = alive;
	}

	public String getAdid() {
		return adid;
	}

	public void setAdid(String adid) {
		this.adid = adid;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	
	

}
