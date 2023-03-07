CREATE TABLE IF NOT EXISTS `CALENDAR`
(
  `DT` date,
  `YEARMONTH` mediumint UNSIGNED,
  `D_YEAR` year,
  `D_MONTH` tinyint(12),
  `D_WEEK` tinyint(53),
  `D_QUARTER` tinyint(4),
  `D_YEARQUARTER` smallint UNSIGNED,
  `DAY_OF_YEAR` smallint UNSIGNED,
  `DAY_OF_WEEK` tinyint(7),
  `DAY_NAME` tinytext
);