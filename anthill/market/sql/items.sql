CREATE TABLE `items` (
  `item_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `owner_id` int(11) unsigned NOT NULL,
  `market_id` int(11) unsigned NOT NULL,
  `item_name` varchar(64) NOT NULL,
  `item_amount` int(11) NOT NULL,
  `item_payload` json DEFAULT NULL,
  `item_hash` varchar(64) NOT NULL,
  PRIMARY KEY (`item_id`),
  UNIQUE KEY `items_UN` (`gamespace_id`,`owner_id`,`market_id`,`item_hash`),
  KEY `items_gamespace_id_IDX` (`gamespace_id`,`owner_id`,`market_id`,`item_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;