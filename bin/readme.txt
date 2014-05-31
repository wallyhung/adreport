push、view、click、download、install日志对应的字段说明：

slot_name  应用发布ID
imei       设备IMEI（便于区分终端用户）
adid       广告ID
type       标示广告类型（1：cpa,2.cpc,3.cpm）
appid      应用ID


request、info日志对应的字段说明：
slot_name     应用发布ID
test_mode     是否为测试模式
imei          客户端设备ID
model         手机型号
manuf         手机品牌
pname         应用包名
app_name      应用名称
net           网络类型 （移动：mobile，联通：unicom，电信：telcom，WIFI：wi）
u_w           屏幕宽度
u_h           屏幕高度
location      地理位置
platfrom      平台(android、ios)
client_sdk    客户端SDK(1.0.0)
format        标示(push)
ua            客户端user-agent
phone_number  电话号码
cust_age      年龄
cust_gender   性别(0:女 1:男 2:未知)
kw            关键字（搜索广告关键字）
extras        扩展参数
channel_id    渠道ID

/** 以下四项为信息收集项  **/
imsi          客户端的imsi号
iccid         客户端的iccid
mac           客户端MAC号
app_list      应用列表
hl            语言


统计结果字段说明：
 广告统计
`oid`       '对应mongo库的objectid'、数据库主键
`adid`      '广告ID',
`push`      '推送数',
`view`      '展示数',
`click`     '点击数',
`download`  '下载数',
`install`   '安装数',
`day`       '统计时间',


应用统计
`oid`     '对应mongo库的objectid',
`fid`     '发布ID',
`request` '请求数',
`view`    '展示数',
`cpc`     'cpc点击数',
`c_wall`  '应用墙点击数',
`c_oth`   '非墙点击数',
`d_wall`  '应用墙下载数',
`d_oth`   '非墙下载数',
`i_wall   '应用墙安装数',
`i_oth`   '非墙安装数',
`new_u`   '新增用户',
`remain`  '留存用户',
`alive`   '日活用户',
`first`   '第一次请求时间',
`day`     '统计时间',

每日总统计
`oid`    '对应mongo主键objectid',
`new_a`  '新增应用数',
`push`   '推送数',
`view`   '展示数',
`click`  '点击数',
`alive`  '终端数',
`day`    '统计时间',




