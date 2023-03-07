CREATE TABLE IF NOT EXISTS `REVENUES` 
(
  `TR_YEAR` year,
  `TR_MONTH` tinyint unsigned,
  `TR_YEARMONTH` mediumint,
  `TR_QUARTER` smallint(4),
  `REV_1M` decimal(16,6),
  `REV_3M` decimal(16,6),
  `REV_6M` decimal(16,6),
  `REV_9M` decimal(16,6),
  `REV_12M` decimal(16,6),
  `REV_QUARTER` decimal(16,6)
);