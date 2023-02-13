DROP DATABASE IF EXISTS `cis555`;
CREATE DATABASE `cis555`;
USE `cis555`;

CREATE TABLE `Documents` (
    `url` VARCHAR(300) NOT NULL,
    `type` VARCHAR(255) NOT NULL,
    `content` MEDIUMTEXT NOT NULL,
    PRIMARY KEY (`url`)
);

CREATE TABLE `IDFs` (
    `term` VARCHAR(255) NOT NULL,
    `idf` DECIMAL(10, 4) NOT NULL,
    PRIMARY KEY (`term`)
);

CREATE TABLE `Inverted_Index` (
    `url` VARCHAR(300) NOT NULL,
    `term` VARCHAR(255) NOT NULL,
    `weight` DECIMAL(10, 4) NOT NULL,
    PRIMARY KEY (`url`, `term`)
);

CREATE TABLE `Links` (
    `src_url` VARCHAR(300) NOT NULL,
    `dest_url` VARCHAR(300) NOT NULL,
    PRIMARY KEY (`src_url`, `dest_url`)
);

CREATE TABLE `Pagerank` (
    `url` VARCHAR(300) NOT NULL,
    `value` DECIMAL(10, 4) NOT NULL,
    PRIMARY KEY (`url`)
);
