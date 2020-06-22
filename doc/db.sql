

CREATE DATABASE IF NOT EXISTS `ethdb` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `ethdb`;


CREATE TABLE IF NOT EXISTS `block` (
  `hash` varchar(100) NOT NULL COMMENT 'block hash ',
  `height` bigint(20) NOT NULL COMMENT 'block height',
  `timestamps` bigint(20) NOT NULL COMMENT 'block create timestamps',
  PRIMARY KEY (`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='The table of block data ';


CREATE TABLE IF NOT EXISTS `chaintx` (
  `txid` varchar(100) NOT NULL,
  `height` bigint(20) NOT NULL,
  PRIMARY KEY (`txid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE IF NOT EXISTS `mempooltx` (
  `txid` varchar(100) NOT NULL,
  `height` bigint(20) NOT NULL,
  PRIMARY KEY (`txid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE IF NOT EXISTS `vin` (
  `txid` varchar(100) NOT NULL,
  `prevtxid` varchar(100) NOT NULL,
  `n` int(11) NOT NULL,
  PRIMARY KEY (`prevtxid`,`n`),
  KEY `txidkey` (`txid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `voutaddress` (
  `txid` varchar(100) NOT NULL,
  `n` int(11) NOT NULL,
  `address` varchar(100) DEFAULT NULL,
  `addresspos` int(11) NOT NULL,
  `value` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`txid`,`n`,`addresspos`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `voutret` (
  `txid` varchar(100) NOT NULL,
  `n` int(11) NOT NULL,
  `data` VARCHAR(512) NOT NULL,
  PRIMARY KEY (`txid`,`n`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `utxo` (
  `txid` varchar(100) NOT NULL,
  `n` int(11) NOT NULL,
  `address` varchar(100) DEFAULT NULL,
  `addresspos` int(11) NOT NULL,
  `value` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`txid`,`n`,`addresspos`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
