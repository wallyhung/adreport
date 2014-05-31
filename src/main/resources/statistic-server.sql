/*
SQLyog Ultimate v11.41 (64 bit)
MySQL - 10.0.10-MariaDB-wsrep-log : Database - statistic
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`statistic` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `statistic`;

/*Table structure for table `ad_day_report` */

DROP TABLE IF EXISTS `ad_day_report`;

CREATE TABLE `ad_day_report` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `adid` varchar(32) NOT NULL COMMENT '广告ID',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `click` bigint(20) DEFAULT '0' COMMENT '点击数',
  `download` bigint(20) DEFAULT '0' COMMENT '下载数',
  `install` bigint(20) DEFAULT '0' COMMENT '安装数',
  `remain` bigint(20) DEFAULT '0' COMMENT '终端数',
  `day` varchar(12) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `ad_day_report_new` */

DROP TABLE IF EXISTS `ad_day_report_new`;

CREATE TABLE `ad_day_report_new` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `adid` varchar(32) NOT NULL COMMENT '广告ID',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `click` bigint(20) DEFAULT '0' COMMENT '点击数',
  `d_wall` bigint(20) DEFAULT '0' COMMENT '积分墙下载数',
  `d_oth` bigint(20) DEFAULT '0' COMMENT '非墙下载数',
  `i_wall` bigint(20) DEFAULT '0' COMMENT '积分墙安装数',
  `i_oth` bigint(20) DEFAULT '0' COMMENT '非墙安装数',
  `remain` bigint(20) DEFAULT '0' COMMENT '终端数',
  `day` varchar(12) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `ad_hour_report` */

DROP TABLE IF EXISTS `ad_hour_report`;

CREATE TABLE `ad_hour_report` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `adid` varchar(32) NOT NULL COMMENT '广告ID',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `click` bigint(20) DEFAULT '0' COMMENT '点击数',
  `download` bigint(20) DEFAULT '0' COMMENT '下载数',
  `install` bigint(20) DEFAULT '0' COMMENT '安装数',
  `remain` bigint(20) DEFAULT '0' COMMENT '终端数',
  `hour` varchar(20) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `ad_hour_report_new` */

DROP TABLE IF EXISTS `ad_hour_report_new`;

CREATE TABLE `ad_hour_report_new` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `adid` varchar(32) NOT NULL COMMENT '广告ID',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `click` bigint(20) DEFAULT '0' COMMENT '点击数',
  `d_wall` bigint(20) DEFAULT '0' COMMENT '积分墙下载数',
  `d_oth` bigint(20) DEFAULT '0' COMMENT '非墙下载数',
  `i_wall` bigint(20) DEFAULT '0' COMMENT '积分墙安装数',
  `i_oth` bigint(20) DEFAULT '0' COMMENT '非墙安装数',
  `remain` bigint(20) DEFAULT '0' COMMENT '终端数',
  `hour` varchar(20) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `app_day_report` */

DROP TABLE IF EXISTS `app_day_report`;

CREATE TABLE `app_day_report` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `fid` varchar(32) NOT NULL COMMENT '发布ID',
  `request` bigint(20) DEFAULT '0' COMMENT '请求数',
  `push` bigint(20) DEFAULT '0',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `cpc` bigint(20) DEFAULT '0' COMMENT 'cpc点击数',
  `c_wall` bigint(20) DEFAULT '0' COMMENT '积分墙推荐点击数',
  `c_oth` bigint(20) DEFAULT '0' COMMENT '非墙点击数',
  `d_wall` bigint(20) DEFAULT '0' COMMENT '积分墙下载数',
  `d_oth` bigint(20) DEFAULT '0' COMMENT '非墙下载数',
  `i_wall` bigint(20) DEFAULT '0' COMMENT '积分墙安装数',
  `i_oth` bigint(20) DEFAULT '0' COMMENT '非墙安装数',
  `new_u` bigint(20) DEFAULT '0' COMMENT '新增用户',
  `remain` bigint(20) DEFAULT '0' COMMENT '留存用户',
  `seven` bigint(20) DEFAULT '0' COMMENT '七日留存',
  `alive` bigint(20) DEFAULT '0' COMMENT '日活用户',
  `first` bigint(20) NOT NULL COMMENT '第一次请求时间',
  `day` varchar(12) DEFAULT NULL COMMENT '统计时间',
  `c_sum` bigint(20) DEFAULT '0' COMMENT 'web cpc合计',
  `i_sum` bigint(20) DEFAULT '0' COMMENT 'web install 合计',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `app_day_report_new` */

DROP TABLE IF EXISTS `app_day_report_new`;

CREATE TABLE `app_day_report_new` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `fid` varchar(32) NOT NULL COMMENT '发布ID',
  `request` bigint(20) DEFAULT '0' COMMENT '请求数',
  `push` bigint(20) DEFAULT NULL COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `cpc` bigint(20) DEFAULT '0' COMMENT 'cpc点击数',
  `c_wall` bigint(20) DEFAULT '0' COMMENT '积分墙推荐点击数',
  `c_oth` bigint(20) DEFAULT '0' COMMENT '非墙点击数',
  `d_wall` bigint(20) DEFAULT '0' COMMENT '积分墙下载数',
  `d_oth` bigint(20) DEFAULT '0' COMMENT '非墙下载数',
  `i_wall` bigint(20) DEFAULT '0' COMMENT '积分墙安装数',
  `i_oth` bigint(20) DEFAULT '0' COMMENT '非墙安装数',
  `new_u` bigint(20) DEFAULT '0' COMMENT '新增用户',
  `remain` bigint(20) DEFAULT '0' COMMENT '留存用户',
  `seven` bigint(20) DEFAULT '0' COMMENT '七日留存',
  `alive` bigint(20) DEFAULT '0' COMMENT '日活用户',
  `first` bigint(20) NOT NULL COMMENT '第一次请求时间',
  `day` varchar(12) DEFAULT NULL COMMENT '统计时间',
  `c_sum` bigint(20) DEFAULT '0' COMMENT 'web cpc合计',
  `i_sum` bigint(20) DEFAULT '0' COMMENT 'web install 合计',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `app_hour_report` */

DROP TABLE IF EXISTS `app_hour_report`;

CREATE TABLE `app_hour_report` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `fid` varchar(32) NOT NULL COMMENT '发布ID',
  `request` bigint(20) DEFAULT '0' COMMENT '请求数',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `cpc` bigint(20) DEFAULT '0' COMMENT 'cpc点击数',
  `c_wall` bigint(20) DEFAULT '0' COMMENT '积分墙推荐点击数',
  `c_oth` bigint(20) DEFAULT '0' COMMENT '非墙点击数',
  `d_wall` bigint(20) DEFAULT '0' COMMENT '积分墙下载数',
  `d_oth` bigint(20) DEFAULT '0' COMMENT '非墙下载数',
  `i_wall` bigint(20) DEFAULT '0' COMMENT '积分墙安装数',
  `i_oth` bigint(20) DEFAULT '0' COMMENT '非墙安装数',
  `new_u` bigint(20) DEFAULT '0' COMMENT '新增用户',
  `remain` bigint(20) DEFAULT '0' COMMENT '留存用户',
  `alive` bigint(20) DEFAULT '0' COMMENT '日活用户',
  `first` bigint(20) NOT NULL COMMENT '第一次请求时间',
  `hour` varchar(20) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `app_hour_report_new` */

DROP TABLE IF EXISTS `app_hour_report_new`;

CREATE TABLE `app_hour_report_new` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo库的objectid',
  `fid` varchar(32) NOT NULL COMMENT '发布ID',
  `request` bigint(20) DEFAULT '0' COMMENT '请求数',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `cpc` bigint(20) DEFAULT '0' COMMENT 'cpc点击数',
  `c_wall` bigint(20) DEFAULT '0' COMMENT '积分墙推荐点击数',
  `c_oth` bigint(20) DEFAULT '0' COMMENT '非墙点击数',
  `d_wall` bigint(20) DEFAULT '0' COMMENT '积分墙下载数',
  `d_oth` bigint(20) DEFAULT '0' COMMENT '非墙下载数',
  `i_wall` bigint(20) DEFAULT '0' COMMENT '积分墙安装数',
  `i_oth` bigint(20) DEFAULT '0' COMMENT '非墙安装数',
  `new_u` bigint(20) DEFAULT '0' COMMENT '新增用户',
  `remain` bigint(20) DEFAULT '0' COMMENT '留存用户',
  `alive` bigint(20) DEFAULT '0' COMMENT '日活用户',
  `first` bigint(20) NOT NULL COMMENT '第一次请求时间',
  `hour` varchar(20) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `day_sum` */

DROP TABLE IF EXISTS `day_sum`;

CREATE TABLE `day_sum` (
  `oid` varchar(50) NOT NULL COMMENT '对应mongo主键objectid',
  `new_a` bigint(20) DEFAULT '0' COMMENT '新增应用数',
  `push` bigint(20) DEFAULT '0' COMMENT '推送数',
  `view` bigint(20) DEFAULT '0' COMMENT '展示数',
  `click` bigint(20) DEFAULT '0' COMMENT '点击数',
  `alive` bigint(20) DEFAULT '0' COMMENT '终端数',
  `day` varchar(12) DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Table structure for table `device_day_report` */

DROP TABLE IF EXISTS `device_day_report`;

CREATE TABLE `device_day_report` (
  `oid` varchar(30) NOT NULL COMMENT 'mongo对应的id',
  `type` int(2) DEFAULT NULL COMMENT '统计类型：1.终端用户2.品牌3.型号4.系统5.地域6.联网方式',
  `field` varchar(20) DEFAULT NULL COMMENT '统计字段',
  `count` bigint(20) DEFAULT '0' COMMENT '统计字段对应的值',
  `day` varchar(10) DEFAULT NULL COMMENT '统计日期',
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
