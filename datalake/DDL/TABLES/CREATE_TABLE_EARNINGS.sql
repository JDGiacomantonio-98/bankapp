CREATE TABLE IF NOT EXISTS `EARNINGS`
(
  `TR_YEAR` year,
  `TR_MONTH` tinyint(12) unsigned,
  `EARN_1M` decimal(16,6),
  `EARN_3M` decimal(16,6),
  `EARN_6M` decimal(16,6),
  `EARN_9M` decimal(16,6),
  `EARN_12M` decimal(16,6),
  `EARN_QUARTER` decimal(16,6)
);