DROP SCHEMA IF EXISTS wikiclassifier;
CREATE SCHEMA wikiclassifier;

DROP TABLE IF EXISTS wikiclassifier.categorylinks;
DROP TABLE IF EXISTS wikiclassifier.page;
DROP TABLE IF EXISTS wikiclassifier.words;

CREATE TABLE wikiclassifier.categorylinks (
  cl_id decimal(8) not null,
  cl_from DECIMAL(8) NOT NULL DEFAULT '0',
  cl_to varchar(255) NOT NULL DEFAULT '',
  cl_sortkey varchar(230) NOT NULL DEFAULT '',
  cl_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  cl_sortkey_prefix varchar(255) NOT NULL DEFAULT '',
  cl_collation VARCHAR(32) NOT NULL DEFAULT '',
  cl_type VARCHAR(6) NOT NULL DEFAULT 'page',
  CONSTRAINT pk_categorylinks PRIMARY KEY (cl_id)
);

-- wiki 2011
CREATE TABLE wikiclassifier.page (
  page_id decimal(8) unsigned NOT NULL AUTO_INCREMENT,
  page_namespace decimal(11) NOT NULL DEFAULT '0',
  page_title varchar(255) NOT NULL DEFAULT '',
  page_restrictions varchar(255) NOT NULL,
  page_counter decimal(20) NOT NULL DEFAULT '0',
  page_is_redirect decimal(1) NOT NULL DEFAULT '0',
  page_is_new decimal(1) NOT NULL DEFAULT '0',
  page_random double NOT NULL DEFAULT '0',
  page_touched varchar(25) NOT NULL DEFAULT '',
  page_latest decimal(8) NOT NULL DEFAULT '0',
  page_len decimal(8) NOT NULL DEFAULT '0',
  page_no_title_convert decimal(1) NOT NULL DEFAULT '0',
  constraint pk_page PRIMARY KEY (page_id),
);

-- wiki 2015
/*
CREATE TABLE wikiclassifier.page (
  page_id DECIMAL(8) NOT NULL,
  page_namespace DECIMAL(11) NOT NULL DEFAULT '0',
  page_title VARCHAR(255) NOT NULL DEFAULT '',
  page_restrictions VARCHAR(256) NOT NULL,
  page_counter DECIMAL(20) NOT NULL DEFAULT '0',
  page_is_redirect DECIMAL(1) NOT NULL DEFAULT '0',
  page_is_new DECIMAL(1) NOT NULL DEFAULT '0',
  page_random DOUBLE NOT NULL DEFAULT '0',
  page_touched VARCHAR(14) NOT NULL DEFAULT '',
  page_links_updated VARCHAR(14) DEFAULT NULL,
  page_latest DECIMAL(8) NOT NULL DEFAULT '0',
  page_len DECIMAL(8) NOT NULL DEFAULT '0',
  page_no_title_convert DECIMAL(1) NOT NULL DEFAULT '0',
  page_content_model VARCHAR(32) DEFAULT NULL,
  CONSTRAINT pk_page PRIMARY KEY (page_id)
);
*/

CREATE TABLE wikiclassifier.words (
  w_id BIGINT NOT NULL PRIMARY KEY,
  w_word VARCHAR(128) NOT NULL
);