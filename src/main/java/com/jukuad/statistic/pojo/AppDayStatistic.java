package com.jukuad.statistic.pojo;


import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Embedded;
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
@Entity(value="app_day", noClassnameStored = true)
public class AppDayStatistic extends BaseEntity
{
	 //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
	@Id
	//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
	@JsonIgnore(value= true)
	private ObjectId id;
	
	/**发布ID(在网站应用提交过程中产生的发布ID)**/
    private String              fid;
    private long                request;
    private long                push;
    private long                view;
    @Embedded
    private Attach              click;
    @Embedded
    private Attach              download;
    @Embedded
    private Attach              install;
    private long                new_u;
    private long                remain;
    private long                seven;
    private long                alive;
    @Indexed(name="idx_day_app", value=IndexDirection.DESC)
    private String                day;
    
    ///~ web data sum
    private long               c_sum;
    private long               i_sum;

    ///~ getter and setter
	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public String getFid() {
		return fid;
	}

	public void setFid(String fid) {
		this.fid = fid;
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


	public Attach getClick() {
		return click;
	}

	public void setClick(Attach click) {
		this.click = click;
	}

	public Attach getDownload() {
		return download;
	}

	public void setDownload(Attach download) {
		this.download = download;
	}

	public Attach getInstall() {
		return install;
	}

	public void setInstall(Attach install) {
		this.install = install;
	}

	public long getNew_u() {
		return new_u;
	}

	public void setNew_u(long new_u) {
		this.new_u = new_u;
	}

	public long getRemain() {
		return remain;
	}

	public void setRemain(long remain) {
		this.remain = remain;
	}

	public long getAlive() {
		return alive;
	}

	public void setAlive(long alive) {
		this.alive = alive;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public long getC_sum() {
		return c_sum;
	}

	public void setC_sum(long c_sum) {
		this.c_sum = c_sum;
	}

	public long getI_sum() {
		return i_sum;
	}

	public void setI_sum(long i_sum) {
		this.i_sum = i_sum;
	}

	public long getSeven() {
		return seven;
	}

	public void setSeven(long seven) {
		this.seven = seven;
	}

}
