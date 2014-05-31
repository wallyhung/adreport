package com.jukuad.statistic.log;

import java.io.Serializable;

/**
 * @author Xull
 * @version 1.0
 * @email wanxkl@gmail.com
 * @created 2014-3-1 下午3:00
 * @description 推送、展示、点击
 */
public class AdFeedback extends BaseEntity implements Serializable{
    private static final long serialVersionUID = 1L;
    private String   slot_name; //发布id
    private String   imei;
    private String   adid;
    private int      type;  //广告类型（cpa,cpm）
    private String   appid;

    public String getSlot_name() {
        return slot_name;
    }

    public void setSlot_name(String slot_name) {
        this.slot_name = slot_name;
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
