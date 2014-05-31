package com.jukuad.statistic.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.jukuad.statistic.log.AdFeedback;
import com.jukuad.statistic.log.ClientMessage;
import com.jukuad.statistic.log.SoftFeedback;
import com.jukuad.statistic.pojo.Click;
import com.jukuad.statistic.pojo.Download;
import com.jukuad.statistic.pojo.Imei;
import com.jukuad.statistic.pojo.Info;
import com.jukuad.statistic.pojo.Install;
import com.jukuad.statistic.pojo.Push;
import com.jukuad.statistic.pojo.Request;
import com.jukuad.statistic.pojo.View;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
/**
 * 采集日志信息转为数据库对象
 * @author Administrator
 *
 */
public class ObjectUtil {
	
	private static IPSeeker ipSeeker = IPSeeker.getInstance();
	
	/**
	 * 请求数据
	 * @param mess
	 * @return
	 */
	public static Request clientMessToRequest(ClientMessage mess)
	{
		if(mess == null) return null;
		else
		{
			Request request = new Request();
			request.setFid(mess.getSlot_name());
			request.setImei(mess.getImei());
			request.setName(mess.getApp_name());
			request.setTimestamp(mess.getTimestamp());
			return request;
		}
	}
	
	public static DBObject clientMessToReqObj(ClientMessage mess)
	{
		if(mess == null) return null;
		else
		{
			DBObject request = new BasicDBObject();
			request.put("fid",mess.getSlot_name());
			request.put("imei",mess.getImei());
			request.put("name",mess.getApp_name());
			request.put("time",mess.getTimestamp());
			return request;
		}
	}
	
	public static Imei clientMessToImei(ClientMessage mess)
	{
		if(mess == null) return null;
		else
		{
			//保存设备信息
			Imei imei = new Imei();
			imei.setBrand(getBrand(mess.getManuf()));
			imei.setIp(mess.getIp());
			imei.setLocation(mess.getLocation());
			imei.setModel(mess.getModel());
			imei.setNet(mess.getNet());
			imei.setPlatform(mess.getPlatfrom());
			imei.setTimestamp(mess.getTimestamp());
			imei.setValue(mess.getImei());
			imei.setUa(mess.getUa());
			imei.setOsVersion(getVersion(mess.getUa()));
			imei.setProvince(getProvince(mess.getIp()));
			return imei;
		}
	}
	
	public static DBObject clientMessToImeiObj(ClientMessage mess)
	{
		if(mess == null) return null;
		else
		{
			//保存设备信息
			DBObject imei = new BasicDBObject();
			imei.put("brand",getBrand(mess.getManuf()));
			imei.put("ip",mess.getIp());
			imei.put("loc",mess.getLocation());
			imei.put("model",mess.getModel());
			imei.put("net",mess.getNet());
			imei.put("mos",mess.getPlatfrom());
			imei.put("time",mess.getTimestamp());
			imei.put("value",mess.getImei());
			imei.put("ua",mess.getUa());
			imei.put("ver",getVersion(mess.getUa()));
			imei.put("prv",getProvince(mess.getIp()));
			return imei;
		}
	}
	
	public static Imei pushToImei(AdFeedback mess)
	{
		if(mess == null) return null;
		else
		{
			//保存设备信息
			Imei imei = new Imei();
			imei.setIp(mess.getIp());
			imei.setTimestamp(mess.getTimestamp());
			imei.setValue(mess.getImei());
			imei.setProvince(getProvince(mess.getIp()));
			return imei;
		}
	}
	
	
	/**
	 * 获取省份信息
	 * @param ip
	 * @return
	 */
	public static String getProvince(String ip)
	{
		if(ip != null && !"".equals(ip))
		{
			try {
				ip = ipSeeker.getCountry(ip);
			} catch (Exception e) {
			}
			if(ip.indexOf("省") > -1) ip = ip.substring(0,ip.indexOf("省"));
			if(ip.indexOf("市") > -1) ip = ip.substring(0,ip.indexOf("市"));
			if(ip.length() > 3)
			{
				if(ip.indexOf("黑龙江") > -1 || ip.indexOf("内蒙古") > -1) ip = ip.substring(0, 3);
				else ip = ip.substring(0, 2);
			}
		}
		return ip;
	}
	
	
	public static String getVersion(String ua)
	{
		if(ua != null && !"".equals(ua))
		{
			ua = ua.substring(Constant.UA_HEAD.length(),ua.length());
			ua = ua.substring(0, ua.indexOf("/"));
		}
		return ua;
	}
	
	public static String getBrand(String brand)
	{
		if(brand != null && !"".equals(brand))
		{
			brand = brand.toLowerCase();
			if(brand.indexOf("华为") > -1 || brand.indexOf("huawei") > -1 || brand.indexOf("hw") > -1)  brand = "华为";
			if(brand.indexOf("酷派") > -1 || brand.indexOf("coolpad") > -1) brand = "酷派";
			if(brand.indexOf("htc") > -1)                                  brand = "htc";
			if(brand.indexOf("小米") > -1 || brand.indexOf("红米") > -1 || brand.indexOf("xiaomi") > -1 ) brand = "小米";
		}
		return brand;
	}
	
	
	public static Info clientMessToInfo(ClientMessage mess)
	{
		if(mess == null) return null;
		else
		{
			Info info = new Info();
			info.setFid(mess.getSlot_name());
			info.setImei(mess.getImei());
			info.setLoc(mess.getLocation());
			info.setName(mess.getApp_name());
			info.setPlat(mess.getPlatfrom());
			info.setTimestamp(mess.getTimestamp());
			info.setBrand(mess.getManuf());
			info.setModel(mess.getModel());
			info.setNet(mess.getNet());
			info.setUa(mess.getUa());
			return info;
		}
	}
	
	
	/**
	 * 推送数据
	 * @param data
	 * @return
	 */
	public static Push adFeedbackToPush(AdFeedback data)
	{
		if(data == null) return null;
		else
		{
			Push push = new Push();
			push.setFid(data.getSlot_name());
			push.setImei(data.getImei());
			push.setAdid(data.getAdid());
			push.setTimestamp(data.getTimestamp());
			return push;
		}
	}
	
	/**
	 * 推送、展示
	 * @param data
	 * @return
	 */
	public static DBObject adFeedbackToPushObj(AdFeedback data)
	{
		if(data == null) return null;
		else
		{
			DBObject push = new BasicDBObject();
			push.put("fid",data.getSlot_name());
			push.put("imei",data.getImei());
			push.put("adid",data.getAdid());
			push.put("time",data.getTimestamp());
			return push;
		}
	}
	
	/**
	 * 展示数据
	 * @param data
	 * @return
	 */
	public static View adFeedbackToView(AdFeedback data)
	{
		if(data == null) return null;
		else
		{
			View view = new View();
			view.setFid(data.getSlot_name());
			view.setImei(data.getImei());
			view.setAdid(data.getAdid());
			view.setTimestamp(data.getTimestamp());
			return view;
		}
	}
	
	
	
	/**
	 * 点击数据
	 * @param data
	 * @return
	 */
	public static Click adFeedbackToClick(AdFeedback data)
	{
		if(data == null) return null;
		else
		{
			Click click = new Click();
			click.setFid(data.getSlot_name());
			click.setImei(data.getImei());
			click.setAdid(data.getAdid());
			click.setTimestamp(data.getTimestamp());
			click.setAppid(data.getAppid());
			click.setType(data.getType());
			return click;
		}
	}
	
	public static DBObject adFeedbackToClickObj(AdFeedback data)
	{
		if(data == null) return null;
		else
		{
			DBObject click = new BasicDBObject();
			click.put("fid",data.getSlot_name());
			click.put("imei",data.getImei());
			click.put("adid",data.getAdid());
			click.put("time",data.getTimestamp());
			click.put("type",data.getType());
			return click;
		}
	}
	
	
	/**
	 * 下载数据
	 * @param data
	 * @return
	 */
	public static Download softFeedbackToDown(SoftFeedback data)
	{
		if(data == null) return null;
		else
		{
			Download down = new Download();
			down.setFid(data.getSlot_name());
			down.setImei(data.getImei());
			down.setAdid(data.getAdid());
			down.setAppid(data.getAppid());
			down.setType(data.getType());
			down.setTimestamp(data.getTimestamp());
			return down;
		}
	}
	
	public static DBObject softFeedbackToDownObj(SoftFeedback data)
	{
		if(data == null) return null;
		else
		{
			DBObject click = new BasicDBObject();
			click.put("fid",data.getSlot_name());
			click.put("imei",data.getImei());
			click.put("adid",data.getAdid());
			click.put("time",data.getTimestamp());
			click.put("appid",data.getAppid());
			click.put("type",data.getType());
			return click;
		}
	}
	
	/**
	 * 安装数据
	 * @param data
	 * @return
	 */
	public static Install softFeedbackToInstall(SoftFeedback data)
	{
		if(data == null) return null;
		else
		{
			Install install = new Install();
			install.setFid(data.getSlot_name());
			install.setImei(data.getImei());
			install.setAdid(data.getAdid());
			install.setAppid(data.getAppid());
			install.setType(data.getType());
			install.setTimestamp(data.getTimestamp());
			return install;
		}
	}
	
	
	/**
	 * 比较两个数组的不同元素的个数
	 * @param list1     大集合
	 * @param list2     小集合
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static int getDiffSize(List list1,List list2)
	{
		int sum = 0;
		Object[] array_1 =  list1.toArray();
		Object[] array_2 =  list2.toArray();
		Arrays.sort(array_1);
		Arrays.sort(array_2);
		
		int len = array_2.length;  
		for (int i = 0; i < len; i++)  
		{  
		    if (Arrays.binarySearch(array_1, array_2[i]) < 0)  sum++;
		}  
		return sum;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static List getDiffList(List list1,List list2)
	{
		List diff = new ArrayList();
		Object[] array_1 =  list1.toArray();
		Object[] array_2 =  list2.toArray();
		Arrays.sort(array_1);
		Arrays.sort(array_2);
		
		int len = array_2.length;  
		for (int i = 0; i < len; i++)  
		{  
		    if (Arrays.binarySearch(array_1, array_2[i]) < 0)  diff.add(array_2[i]);
		}  
		return diff;
	}
	
	
	
	public static void main(String[] args) {
		List<String> list= new ArrayList<String>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");
		
		List<String> list1= new ArrayList<String>();
		list1.add("d");
		list1.add("b");
		list1.add("c");
		
		System.out.println("不同的数据个数：" + getDiffSize(list1, list));
		System.out.println("---不同的数据个数：" + getDiffSize(list, list1));
		
	}

}
