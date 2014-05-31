package com.jukuad.statistic.pojo;


import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.utils.IndexDirection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * 每天的统计结果
 * @author Administrator
 *
 */
@Entity(value="app_res", noClassnameStored = true)
public class AppResult extends BaseEntity
{
	 //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
	@Id
	//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
	@JsonIgnore(value= true)
	private ObjectId id;
	
	/**发布ID(在网站应用提交过程中产生的发布ID)**/
    private String              fid;
    private long                push;
    private long                request;
    private long                view;
    @Embedded
    private Attach              click;
    @Embedded
    private Attach              download;
    @Embedded
    private Attach              install;
    
    //Jackson中的注解 标明该字段使用自定义的DateSerializer类实现序列化  
    @JsonSerialize(using= DateSerializer.class)  
    //Jackson中的注解 标明该字段使用自定义的DateDeserializer类实现反序列化  
    @JsonDeserialize(using= DateDeserializer.class)
    @Property(value="first")
    private long                first;
    
    private long                remain;
    
    private long                new_u;
    @Indexed(name="idx_hour_app",value=IndexDirection.DESC)
    private String hour;
    

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

	public long getFirst() {
		return first;
	}

	public void setFirst(long first) {
		this.first = first;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public long getRemain() {
		return remain;
	}

	public void setRemain(long remain) {
		this.remain = remain;
	}

	public long getNew_u() {
		return new_u;
	}

	public void setNew_u(long new_u) {
		this.new_u = new_u;
	}
	
	

}
