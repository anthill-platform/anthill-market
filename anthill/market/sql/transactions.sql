
CREATE TABLE `transactions` (
  `transaction_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `gamespace_id` int(11) unsigned NOT NULL,
  `market_id` int(11) unsigned NOT NULL,
  `transaction_give_item` varchar(64) NOT NULL,
  `transaction_give_payload` json DEFAULT NULL,
  `transaction_give_hash` varchar(64) NOT NULL,
  `transaction_give_amount` int(11) unsigned NOT NULL,
  `transaction_give_owner` int(11) unsigned NOT NULL,
  `transaction_amount` int(11) unsigned NOT NULL,
  `transaction_take_item` varchar(64) NOT NULL,
  `transaction_take_payload` json DEFAULT NULL,
  `transaction_take_hash` varchar(64) NOT NULL,
  `transaction_take_amount` int(11) unsigned NOT NULL,
  `transaction_take_owner` int(11) unsigned NOT NULL,
  `transaction_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`transaction_id`),
  KEY `transactions_transaction_id_IDX` (`transaction_id`) USING BTREE,
  KEY `transactions_give` (`gamespace_id`,`market_id`,`transaction_give_item`) USING BTREE,
  KEY `transactions_take` (`gamespace_id`,`market_id`,`transaction_take_item`) USING BTREE,
  KEY `transactions_give_hash` (`gamespace_id`,`market_id`,`transaction_give_hash`) USING BTREE,
  KEY `transactions_take_hash` (`gamespace_id`,`market_id`,`transaction_take_hash`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;