package com.jukuad.statistic.pojo;


import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * 每天总统计结果
 * @author Administrator
 *
 */
@Entity(value="sum", noClassnameStored = true)
public class DaySum extends BaseEntity
{
	 //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
	@Id
	//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
	@JsonIgnore(value= true)
	private ObjectId id;
	
    private long                push;
    private long                view;
    private long                click;
    private long                new_a;
    private long                alive;  //终端数
    
    @Property(value="day")
    private String                day;

    ///~ getter and setter
	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
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

	public long getNew_a() {
		return new_a;
	}

	public void setNew_u(long new_a) {
		this.new_a = new_a;
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

	
	

}
