package com.jukuad.statistic.log;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Xull
 * @version 1.0
 * @email wanxkl@gmail.com
 * @created 2014-2-24 下午8:04
 * @description  广告请求事件、记录请求数、信息采集、
 */
public class ClientMessage extends BaseEntity implements Serializable{
    private static final long serialVersionUID = 1L;
    /**发布ID(在网站应用提交过程中产生的发布ID)**/
    private String              slot_name;
    /**是否为测试模式**/
    private boolean             test_mode;
    /**客户端设备ID**/
    private String              imei;
    /**手机型号**/
    private String              model;
    /**手机品牌**/
    private String              manuf;
    /**应用包名**/
    private String              pname;
    /** 应用名称**/
    private String              app_name;
    /** 网络类型 （移动：mobile，联通：unicom，电信：telcom，WIFI：wi）**/
    private String              net;
    /** 屏幕宽度 **/
    private int                 u_w;
    /** 屏幕高度 **/
    private int                 u_h;
    /** 地理位置 **/
    private String              location;
    /** 平台(android、ios) **/
    private String              platfrom;
    /** 客户端SDK(1.0.0) **/
    private String              client_sdk;
    /** 标示(push) **/
    private String              format;
    /** 客户端user-agent **/
    private String              ua;
    /** 电话号码 **/
    private String                 phone_number;
    /** 年龄 **/
    private int                 cust_age;
    /** 性别(0:女 1:男 2:未知) **/
    private int                 cust_gender;
    /** 关键字（搜索广告关键字） **/
    private List<String>              kw;
    /** 扩展参数 **/
    private Map<String, Object> extras;
    /** 渠道ID **/
    private int                 channel_id;

    /** 以下四项为信息收集项  **/
    /** 客户端的imsi号 **/
    private String       imsi;
    /** 客户端的iccid **/
    private String       iccid;
    /** 客户端MAC号 **/
    private String       mac;
    /** 应用列表 **/
    private List<String> app_list;
    /** 语言 **/
    private String hl;

    public ClientMessage() {
    }

    public String getSlot_name() {
        return slot_name;
    }

    public void setSlot_name(String slot_name) {
        this.slot_name = slot_name;
    }

    public boolean isTest_mode() {
        return test_mode;
    }

    public void setTest_mode(boolean test_mode) {
        this.test_mode = test_mode;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getManuf() {
        return manuf;
    }

    public void setManuf(String manuf) {
        this.manuf = manuf;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getNet() {
        return net;
    }

    public void setNet(String net) {
        this.net = net;
    }

    public int getU_w() {
        return u_w;
    }

    public void setU_w(int u_w) {
        this.u_w = u_w;
    }

    public int getU_h() {
        return u_h;
    }

    public void setU_h(int u_h) {
        this.u_h = u_h;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getPlatfrom() {
        return platfrom;
    }

    public void setPlatfrom(String platfrom) {
        this.platfrom = platfrom;
    }

    public String getClient_sdk() {
        return client_sdk;
    }

    public void setClient_sdk(String client_sdk) {
        this.client_sdk = client_sdk;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getPhone_number() {
        return phone_number;
    }

    public void setPhone_number(String phone_number) {
        this.phone_number = phone_number;
    }

    public int getCust_age() {
        return cust_age;
    }

    public void setCust_age(int cust_age) {
        this.cust_age = cust_age;
    }

    public int getCust_gender() {
        return cust_gender;
    }

    public void setCust_gender(int cust_gender) {
        this.cust_gender = cust_gender;
    }

    public List<String> getKw() {
        return kw;
    }

    public void setKw(List<String> kw) {
        this.kw = kw;
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    public void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    public int getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(int channel_id) {
        this.channel_id = channel_id;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getIccid() {
        return iccid;
    }

    public void setIccid(String iccid) {
        this.iccid = iccid;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public List<String> getApp_list() {
        return app_list;
    }

    public void setApp_list(List<String> app_list) {
        this.app_list = app_list;
    }

    public String getHl() {
        return hl;
    }

    public void setHl(String hl) {
        this.hl = hl;
    }


}
