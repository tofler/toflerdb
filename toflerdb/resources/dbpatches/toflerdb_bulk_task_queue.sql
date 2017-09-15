DROP TABLE IF EXISTS `toflerdb_bulk_task_queue`;
CREATE TABLE `toflerdb_bulk_task_queue` (
    `user_id` INT,
    `public_taskid` CHAR(16) PRIMARY KEY,
    `internal_taskid` INT NOT NULL,
    `task_queue_id` INT NOT NULL,
    `task_status` CHAR(1),
    `task_info` BLOB DEFAULT NULL,
    INDEX `userindex` (user_id),
    INDEX `inttaskindex` (internal_taskid),
    INDEX `taskqidindex` (task_queue_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;