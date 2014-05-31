package com.jukuad.statistic.log;

import java.io.Serializable;

/**
 * @author Xull
 * @version 1.0
 * @email wanxkl@gmail.com
 * @created 2014-3-1 下午3:00
 * @description 下载、安装
 */
public class SoftFeedback extends BaseEntity implements Serializable{
    private static final long serialVersionUID = 1L;
    private String   slot_name;
    private String   imei;
    private String   adid;
    private String   appid;
    private int      type;
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

    public String getAppid() {
        return appid;
    }

        public void setAppid(String appid) {
        this.appid = appid;
    }

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}
}
