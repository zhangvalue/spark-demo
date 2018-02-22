/*
Navicat MySQL Data Transfer

Source Server         : 127.0.0.1-3306
Source Server Version : 50631
Source Host           : localhost:3306
Source Database       : big_data

Target Server Type    : MYSQL
Target Server Version : 50631
File Encoding         : 65001

Date: 2018-02-22 17:35:08
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for ta
-- ----------------------------
DROP TABLE IF EXISTS `ta`;
CREATE TABLE `ta` (
  `id` varchar(255) DEFAULT NULL,
  `code` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of ta
-- ----------------------------
INSERT INTO `ta` VALUES ('1', '0001');
INSERT INTO `ta` VALUES ('2', '2');
INSERT INTO `ta` VALUES ('3', '4');
INSERT INTO `ta` VALUES ('3', '4');
