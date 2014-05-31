package com.jukuad.statistic.log;

/**
 * @author Xull
 * @version 1.0
 * @email wanxkl@gmail.com
 * @created 2014-2-27
 * @description
 */
public class BaseEntity {
    private String ip;
    private String real_ua;
    private long timestamp;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getReal_ua() {
        return real_ua;
    }

    public void setReal_ua(String real_ua) {
        this.real_ua = real_ua;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
