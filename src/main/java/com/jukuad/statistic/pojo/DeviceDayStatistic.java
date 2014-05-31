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
@Entity(value="device_day", noClassnameStored = true)
public class DeviceDayStatistic extends BaseEntity
{
	 //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
	@Id
	//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
	@JsonIgnore(value= true)
	private ObjectId id;
    private int      type;  //统计类型
    private String   field; //统计字段
    private long     count; //统计字段的值
    @Indexed(name="idx_day_device", value=IndexDirection.DESC)
    private String   day;   //统计日期
    
    
    ///~ getter and setter
	public ObjectId getId() {
		return id;
	}
	public void setId(ObjectId id) {
		this.id = id;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}

}
