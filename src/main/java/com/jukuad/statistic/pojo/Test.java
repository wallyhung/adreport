package com.jukuad.statistic.pojo;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Property;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity(value="test", noClassnameStored = true)
@Indexes(@Index(name="idx_test_sec", value="ip,value,-time"))
public class Test extends BaseEntity
{
	   //morphia中的注解 标明该key为标识字段(MongoDB中特殊的ObjectId字段)
		@Id
		//Jackson中的注解 标明在序列化与反序列化过程中不使用该key
		@JsonIgnore(value= true)
		private ObjectId id;
		/**发布ID(在网站应用提交过程中产生的发布ID)**/
	    private String              ip;      //手机ip
	    @Indexed(name="idx_test")
	    private String              value;
	    @Property(value="prv")
	    private String             province;  //根据ip获取的省份或机构
	    
	    private int i;

		public ObjectId getId() {
			return id;
		}

		public void setId(ObjectId id) {
			this.id = id;
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

		public String getProvince() {
			return province;
		}

		public void setProvince(String province) {
			this.province = province;
		}

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}
}