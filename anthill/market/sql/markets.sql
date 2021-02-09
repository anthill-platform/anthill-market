CREATE TABLE `markets` (
  `gamespace_id` int(11) unsigned NOT NULL,
  `market_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `market_name` varchar(64) NOT NULL,
  `market_settings` json NOT NULL,
  PRIMARY KEY (`market_id`),
  UNIQUE KEY `markets_UN` (`market_name`,`gamespace_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;