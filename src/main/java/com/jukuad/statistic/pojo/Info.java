package com.jukuad.statistic.pojo;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;

import com.fasterxml.jackson.annotation.JsonIgnore;

//morphia中的注解 标明该对象存入request集合中 并且不存储类名  
@Entity(value="info", noClassnameStored = true)  
@Indexes(@Index(name="idx_distinct_info", value="fid,imei,-time"))
public class Info extends BaseEntity
{
	    //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
		@Id
		//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
		@JsonIgnore(value= true)
		private ObjectId id;
		
		/**发布ID(在网站应用提交过程中产生的发布ID)**/
	    private String              fid;
	    /**手机型号***/
	    private  String             model;
	    /**手机品牌**/
	    private String              brand;
	    /** 网络类型 （移动：mobile，联通：unicom，电信：telcom，WIFI：wi）**/
	    private String              net;
	    /**客户端设备ID**/
	    private String              imei;
	    /** 应用名称**/
	    private String              name;
	    /** 地理位置 **/
	    private String              loc;
	    /** 平台(android、ios) **/
	    private String              plat;
	    /** 客户端user-agent **/
	    private String              ua;
	    

		
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

		public String getImei() {
			return imei;
		}

		public void setImei(String imei) {
			this.imei = imei;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getLoc() {
			return loc;
		}

		public void setLoc(String loc) {
			this.loc = loc;
		}

		public String getPlat() {
			return plat;
		}

		public void setPlat(String plat) {
			this.plat = plat;
		}

		public String getModel() {
			return model;
		}

		public void setModel(String model) {
			this.model = model;
		}

		public String getBrand() {
			return brand;
		}

		public void setBrand(String brand) {
			this.brand = brand;
		}

		public String getNet() {
			return net;
		}

		public void setNet(String net) {
			this.net = net;
		}

		public String getUa() {
			return ua;
		}

		public void setUa(String ua) {
			this.ua = ua;
		}

		
}
