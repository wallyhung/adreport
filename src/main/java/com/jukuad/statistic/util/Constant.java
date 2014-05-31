package com.jukuad.statistic.util;

public class Constant {
	
	/**点击日志路径**/
	public final static String PATH_CLICK = "click";
	/**下载日志路径**/
	public final static String PATH_DOWNLOAD = "download";
	/**客户端信息采集日志路径**/
	public final static String PATH_INFO = "exception";
	/**安装日志路径**/
	public final static String PATH_INSTALL = "install";
	/**推送日志路径**/
	public final static String PATH_PUSH = "push";
	/**请求日志路径**/
	public final static String PATH_REQUEST = "request";
	/**展示日志路径**/
	public final static String PATH_VIEW = "view";
	/**错误日志路径**/
	public final static String PATH_EXCEPTION = "exception";
	/****ua头****/
	public final static String UA_HEAD = "Juku.com/Android/";
	
	
	/***统计类型**/
	public final static int REPORT_TYPE_TERMINAL = 1;
	public final static int REPORT_TYPE_BRAND = 2;
	public final static int REPORT_TYPE_MODEL = 3;
	public final static int REPORT_TYPE_OS = 4;
	public final static int REPORT_TYPE_LOCATION = 5;
	public final static int REPORT_TYPE_NET = 6;
	
	/**统计标示**/
	public final static int FLAG_APP = 1;
	public final static int FLAG_AD = 2;
	
	
	/***统计标示**/
	public final static int SIGN_DAY = 0;      //日活用户
	public final static int SIGN_REMAIN = 1;   //留存用户
	public final static int SIGN_NEW = 2;      //新增用户
	
	
	public final static long TEMPSTAMP_ONE = 86400000;
	public final static long TEMPSTAMP_THREE = 259200000;
	public final static long TEMPSTAMP_HOUR = 3600000;

	public final static int CPA = 1;
	public final static int CPC = 2;
	public final static int CPM = 3;
	
			
}
