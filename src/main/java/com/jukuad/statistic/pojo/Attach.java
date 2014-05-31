package com.jukuad.statistic.pojo;

import org.mongodb.morphia.annotations.Embedded;

@Embedded
public class Attach 
{
	private long cpc;   //cpc点击
	private long wall;  //积分墙推荐
	private long oth;   //非积分墙
	
	public Attach() {
	}
	
	public Attach(long wall,long oth) {
		this.wall = wall;
		this.oth = oth;
	}
	
	public Attach(long cpc,long wall,long oth) {
		this.cpc = cpc;
		this.wall = wall;
		this.oth = oth;
	}
	
	public long getCpc() {
		return cpc;
	}
	public void setCpc(long cpc) {
		this.cpc = cpc;
	}
	public long getWall() {
		return wall;
	}
	public void setWall(long wall) {
		this.wall = wall;
	}
	public long getOth() {
		return oth;
	}
	public void setOth(long oth) {
		this.oth = oth;
	}

}
