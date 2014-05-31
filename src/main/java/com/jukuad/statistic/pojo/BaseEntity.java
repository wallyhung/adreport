package com.jukuad.statistic.pojo;

import org.mongodb.morphia.annotations.Property;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class BaseEntity {
    //Jackson中的注解 标明该字段使用自定义的DateSerializer类实现序列化  
    @JsonSerialize(using= DateSerializer.class)  
    //Jackson中的注解 标明该字段使用自定义的DateDeserializer类实现反序列化  
    @JsonDeserialize(using= DateDeserializer.class)
    @Property(value="time")
    private long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
