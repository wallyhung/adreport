package com.jukuad.statistic.pojo;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity(value="click", noClassnameStored = true)
public class Click extends BaseEntity
{
	    //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
		@Id
		//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
		@JsonIgnore(value= true)
		private ObjectId id;
		
		/**发布ID(在网站应用提交过程中产生的发布ID)**/
	    private String              fid;
	    /**客户端设备ID**/
	    private String              imei;
	    /** 广告ID**/
	    private String              adid;
	    
	    private int                 type;
	    
	    private String              appid;
			
		//morphia中的注解 指示在存入MongoDB之前的操作
//		@PrePersist
//		private void beforeSaving(){
//			System.out.println("即将保存对象:"+ this.toString());
//		}
		
		//morphia中的注解 指示在存入MongoDB之后的操作
//		 @PostPersist  
//		 private void afterSaving(){  
//		    System.out.println("对象保存完毕:"+ this.toString());  
//		}  
		
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

		public String getAdid() {
			return adid;
		}

		public void setAdid(String adid) {
			this.adid = adid;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public String getAppid() {
			return appid;
		}

		public void setAppid(String appid) {
			this.appid = appid;
		}

		
		
}