DROP TABLE IF EXISTS toflerdb_eternity;

CREATE TABLE IF NOT EXISTS `toflerdb_eternity` (
    `fact_id` CHAR(16) PRIMARY KEY,
    `subject` CHAR(16) DEFAULT NULL,
    `predicate` VARCHAR(100) DEFAULT NULL,
    `object` CHAR(16) DEFAULT NULL,
    `value` BLOB DEFAULT NULL,
    `uid` varchar(20) DEFAULT NULL,
    `source` VARCHAR(256) DEFAULT NULL,
    `prev` CHAR(16) DEFAULT NULL,
    `ts` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `processed` BOOLEAN DEFAULT NULL,
    `status` CHAR(1),
    `status_updated_on` DATETIME DEFAULT NULL,
    `status_updated_by` INT DEFAULT NULL,
    INDEX `subjectindex` (subject),
    INDEX `predicateindex` (predicate),
    INDEX `objectindex` (object)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;