/*
SQLyog Ultimate v11.11 (64 bit)
MySQL - 5.5.5-10.0.9-MariaDB : Database - statistic
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

/* Trigger structure for table `ad_day_report` */

DELIMITER $$

/*!50003 DROP TRIGGER*//*!50032 IF EXISTS */ /*!50003 `ad_day_report_AINS` */$$

/*!50003 CREATE */ /*!50017 DEFINER = 'root'@'localhost' */ /*!50003 TRIGGER `ad_day_report_AINS` AFTER INSERT ON `ad_day_report` FOR EACH ROW BEGIN
  -- 广告单价
  DECLARE var_price float;
  -- 广告标题
  DECLARE var_title varchar(255);
  -- 广告类型 1：cpa 2：cpc
  DECLARE var_type int;
  -- 预计比例
  DECLARE var_estimate float;
  -- 效果数
  DECLARE var_valid float;
  -- 日期
  DECLARE var_create_time float;
  -- 统计表的ID
  DECLARE var_id int;
  -- 查询出基础数据
  SELECT
    price,
    title,
    `type` INTO var_price, var_title, var_type
  FROM `adpushadmin`.`ad`
  WHERE `adpushadmin`.`ad`.adid = new.adid;
  -- 判断基础数据是否为空，为空证明没有对应的adid（广告id）
  IF (!ISNULL(var_price) || !ISNULL(var_title)) THEN
    -- 判断是否是1：cpa 如果是则向cpa统计表插入
    IF (var_type = 1) THEN
      -- 查出最后一天的统计数据的效果数和预计比例
      SELECT
        id,
        estimate,
        void,
        create_time INTO var_id, var_estimate, var_valid, var_create_time
      FROM `adpushadmin`.`ad_summary_cpa`
      WHERE create_time = (SELECT
          MAX(create_time)
        FROM `adpushadmin`.`ad_summary_cpa`
        WHERE adid = new.adid) AND adid = new.adid;

      -- 如果id是空则表明此广告还未产生数据，此广告是新广告，新广告的预计比例按照在投放广告最后一天的数据预计比例的平均值

      SELECT
        AVG(estimate) INTO var_estimate
      FROM `adpushadmin`.`ad_summary_cpa`
      WHERE create_time = (SELECT
          MAX(create_time)
        FROM `adpushadmin`.`ad_summary_cpa`);

      IF (DATE_FORMAT(var_create_time, '%Y-%m-%d') = new.day) THEN
        UPDATE `adpushadmin`.`ad_summary_cpa`
        SET `click` = new.click,
            `click_rate` = new.click / new.push * 100,
            `download` = new.download,
            `download_rate` = new.download / new.click * 100,
            `estimate` = var_estimate,
            `install` = new.install,
            `install_rate` = new.install / new.click,
            `price` = var_price,
            `push` = new.push,
            `title` = var_title,
            `valid` = var_valid
        WHERE `id` = var_id;
      ELSE
        INSERT INTO `adpushadmin`.`ad_summary_cpa` (`adid`,
        `click`,
        `click_rate`,
        `create_time`,
        `download`,
        `download_rate`,
        `estimate`,
        `install`,
        `install_rate`,
        `price`,
        `push`,
        `title`,
        `valid`)
          VALUES (new.adid, new.click, new.click / new.push * 100, new.day, new.download, new.download / new.click * 100, var_estimate, new.install, new.install / new.click, var_price, new.push, var_title, var_void);
      END IF;
    ELSE -- 判断不是1 则向cpc统计表插入，未完成，期待赵华安给力登场。
      INSERT INTO `adpushadmin`.`ad_summary_cpc` (`adid`,
      `click`,
      `click_rate`,
      `create_time`,
      `estimate`,
      `price`,
      `push`,
      `title`,
      `valid`)
        VALUES (new.adid, new.click, new.click / new.push * 100, new.day, var_estimate, var_price, new.push, var_title, 0);
    END IF;
  END IF;
END */$$


DELIMITER ;

/* Trigger structure for table `app_day_report` */

DELIMITER $$

/*!50003 DROP TRIGGER*//*!50032 IF EXISTS */ /*!50003 `app_day_report_AINS` */$$

/*!50003 CREATE */ /*!50017 DEFINER = 'root'@'localhost' */ /*!50003 TRIGGER `app_day_report_AINS` AFTER INSERT ON `app_day_report` FOR EACH ROW BEGIN
  DECLARE var_account int ;
  DECLARE var_app_name INT ;
  DECLARE var_cpm INT ;
  DECLARE var_decrease INT ;
  DECLARE var_new_app INT ;
  DECLARE var_settle INT ;
  
  SELECT 
    adpushadmin.app.`name`,`admin_account`  INTO var_app_name, var_account
  FROM
    adpushadmin.app ,adpushadmin.developer
  WHERE slot_name = new.fid and  dev_id=adpushadmin.developer.id;
  
  INSERT INTO `adpushadmin`.`app_summary` (
    `account`,
    `active_user`,
    `app_name`,
    `clickaone`,
    `clickatwo`,
    `clickc`,
    `cpm`,
    `create_time`,
    `decrease`,
    `download_one`,
    `download_two`,
    `impression`,
    `install_one`,
    `install_two`,
    `new_app`,
    `new_user`,
    `request`,
    `retain_user`,
    `settle`,
    `slot_name`
  ) 
  VALUES
    (
      var_account,
      new.alive,
      var_app_name,
      new.c_oth,
      new.c_wall,
      new.cpc,
      var_cpm,
      new.day,
      var_decrease,
      new.d_oth,
      new.d_wall,
      new.view,
      new.i_oth,
      new.i_wall,
      var_new_app,
      new.new_u,
      new.request,
      new.remain,
      var_settle,
      new.fid
    ) ;
END */$$


DELIMITER ;

/* Trigger structure for table `day_sum` */

DELIMITER $$

/*!50003 DROP TRIGGER*//*!50032 IF EXISTS */ /*!50003 `day_sum_AINS` */$$

/*!50003 CREATE */ /*!50017 DEFINER = 'root'@'localhost' */ /*!50003 TRIGGER `day_sum_AINS` AFTER INSERT ON `day_sum` FOR EACH ROW BEGIN
-- 填充率还有问题 广告展示数没有
INSERT INTO `adpushadmin`.`day_summary`
            (
             `click`,
             `click_rate`,
             `create_date`,
             `device`,
             `fill_rate`,
             `impression`,
             `new_device`,
             `push`)
VALUES (
        new.click,
        new.click/new.view*100,
        new.day,
        new.alive,
        new.view/new.push*100,
        new.view,
        new.new_a,
        new.push);
    END */$$


DELIMITER ;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
