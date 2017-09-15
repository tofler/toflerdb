DROP TABLE IF EXISTS toflerdb_ontology;

CREATE TABLE IF NOT EXISTS `toflerdb_ontology` (
    `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `subject` varchar(100) DEFAULT NULL,
    `predicate` varchar(100) DEFAULT NULL,
    `object` varchar(100) DEFAULT NULL,
    `value` varchar(100) DEFAULT NULL,
    `uid` varchar(20) DEFAULT NULL,
    `ts` datetime DEFAULT CURRENT_TIMESTAMP,
    `processed` boolean DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;