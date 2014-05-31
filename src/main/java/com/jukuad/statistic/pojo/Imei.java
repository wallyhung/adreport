package com.jukuad.statistic.pojo;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity(value="imei", noClassnameStored = true)
public class Imei extends BaseEntity
{
	   //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
		@Id
		//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
		@JsonIgnore(value= true)
		private ObjectId id;
		
		/**发布ID(在网站应用提交过程中产生的发布ID)**/
	    private String              fid;
	    private String              adid;
	    private String              ip;      //手机ip
	    private String              value;
	    private String              brand;   //品牌
	    private String              model;   //型号
	    @Property(value="mos")
	    private String              platform; //操作系统
	    private String              net;   //联网方式
	    @Property(value="loc")
	    private String              location;
	    private String              ua;
	    @Property(value="ver")
	    private String              osVersion;  //系统版本号
	    @Property(value="prv")
	    private String             province;  //根据ip获取的省份或机构
	    @Transient
	    private int  flag;
	    
	    public Imei() {
		}
	    
	    public Imei(String value) {
	    	this.value = value;
		}
	    
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

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public String getAdid() {
			return adid;
		}

		public void setAdid(String adid) {
			this.adid = adid;
		}

		public String getBrand() {
			return brand;
		}

		public void setBrand(String brand) {
			this.brand = brand;
		}

		public String getModel() {
			return model;
		}

		public void setModel(String model) {
			this.model = model;
		}

		public String getPlatform() {
			return platform;
		}

		public void setPlatform(String platform) {
			this.platform = platform;
		}

		public String getNet() {
			return net;
		}

		public void setNet(String net) {
			this.net = net;
		}

		public String getLocation() {
			return location;
		}

		public void setLocation(String location) {
			this.location = location;
		}

		public String getUa() {
			return ua;
		}

		public void setUa(String ua) {
			this.ua = ua;
		}


		public String getOsVersion() {
			return osVersion;
		}


		public void setOsVersion(String osVersion) {
			this.osVersion = osVersion;
		}


		public String getProvince() {
			return province;
		}

		public void setProvince(String province) {
			this.province = province;
		}

		public int getFlag() {
			return flag;
		}

		public void setFlag(int flag) {
			this.flag = flag;
		}
}
