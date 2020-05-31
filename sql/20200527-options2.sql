CREATE TABLE `options2` (
`convid` BIGINT NOT NULL ,
`sticker_prob` DOUBLE NULL DEFAULT NULL ,
`reply_prob` DOUBLE NULL DEFAULT NULL ,
`admin_only` TINYINT NULL DEFAULT NULL ,
`blacklisted` TINYINT NULL DEFAULT NULL ,
`silent_commands` TINYINT NULL DEFAULT NULL ,
`is_bad` INT NULL DEFAULT NULL ,
`is_hidden` INT NULL DEFAULT NULL ,
PRIMARY KEY (`convid`)) ENGINE = ROCKSDB; 

ALTER TABLE `options2` ADD `send_as_reply` INT NULL DEFAULT NULL AFTER `is_hidden`;

