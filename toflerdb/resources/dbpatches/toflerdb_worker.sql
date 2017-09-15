DROP TABLE IF EXISTS `toflerdb_worker`;

CREATE TABLE `toflerdb_worker` (
  `worker_id` int(11) NOT NULL,
  `assigned_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `lease_period` int(11) DEFAULT '300',
  PRIMARY KEY (`worker_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
