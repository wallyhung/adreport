package com.jukuad.statistic.pojo;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import com.fasterxml.jackson.annotation.JsonIgnore;

//morphia中的注解 标明该对象存入request集合中 并且不存储类名  
@Entity(value="request", noClassnameStored = true)  
public class Request extends BaseEntity
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
	    /** 应用名称**/
	    private String              name;
		
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
}
