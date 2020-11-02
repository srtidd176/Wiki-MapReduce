-- Create database for wikipedia questions
CREATE DATABASE WikiDB;


SET hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode=nonstrict;

USE WikiDB;

-- Create external table for raw data.
-- This is for the pageviews from October 20th 2020
CREATE EXTERNAL TABLE IF NOT EXISTS WIKI_VIEWS_OCT
  (DOMAIN STRING,
  WIKI_TITLE STRING,
  VIEWS INT,
  RESPONSE_SIZE INT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY " "
  TBLPROPERTIES("skip.header.line.count"="1");

-- Create external table for raw data
-- This is for the pageviews from the month of September 2020
CREATE EXTERNAL TABLE IF NOT EXISTS WIKI_VIEWS_SEP
  (DOMAIN STRING,
  WIKI_TITLE STRING,
  VIEWS INT,
  RESPONSE_SIZE INT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY " "
  TBLPROPERTIES("skip.header.line.count"="1");

-- Create external table for raw data
-- This is for the clickstream data from the month of September 2020
CREATE EXTERNAL TABLE IF NOT EXISTS WIKI_CLICKSTREAM
  (WIKI_TITLE STRING,
  LINK_PAGE STRING,
  LINK STRING,
  COUNT INT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY "\t"
  TBLPROPERTIES("skip.header.line.count"="1");


-- Create exteranal table for raw data
-- This is for the wikipedia edit logs from the month of September 2020
CREATE EXTERNAL TABLE IF NOT EXISTS WIKI_EDITS
  (
    wiki_db STRING,
 event_entity STRING,
 event_type STRING,
 event_timestamp STRING,
 event_comment STRING,

 event_user_id BIGINT,
 event_user_text_historical STRING,
 event_user_text STRING,
 event_user_blocks_historical STRING,
 event_user_blocks STRING,
 event_user_groups_historical STRING,
 event_user_groups STRING,
 event_user_is_bot_by_historical STRING,
 event_user_is_bot_by STRING,
 event_user_is_created_by_self BOOLEAN,
 event_user_is_created_by_system BOOLEAN,
 event_user_is_created_by_peer BOOLEAN,
 event_user_is_anonymous BOOLEAN,
 event_user_registration_timestamp STRING,
 event_user_creation_timestamp STRING,
 event_user_first_edit_timestamp STRING,
 event_user_revision_count BIGINT,
 event_user_seconds_since_previous_revision BIGINT,

 page_id BIGINT,
 page_title_historical STRING,
 page_title STRING,
 page_namespace_historical INT,
 page_namespace_is_content_historical BOOLEAN,
 page_namespace INT,
 page_namespace_is_content BOOLEAN,
 page_is_redirect BOOLEAN,
 page_is_deleted BOOLEAN,
 page_creation_timestamp STRING,
 page_first_edit_timestamp STRING,
 page_revision_count BIGINT,
 page_seconds_since_previous_revision BIGINT,

 user_id BIGINT,
 user_text_historical  STRING,
 user_text STRING,
 user_blocks_historical STRING,
 user_blocks STRING,
 user_groups_historical STRING,
 user_groups STRING,
 user_is_bot_by_historical STRING,
 user_is_bot_by STRING,
 user_is_created_by_self BOOLEAN,
 user_is_created_by_system BOOLEAN,
 user_is_created_by_peer BOOLEAN,
 user_is_anonymous BOOLEAN,
 user_registration_timestamp STRING,
 user_creation_timestamp STRING,
 user_first_edit_timestamp STRING,

 revision_id BIGINT,
 revision_parent_id BIGINT,
 revision_minor_edit BOOLEAN,
 revision_deleted_parts STRING,
 revision_deleted_parts_are_suppressed BOOLEAN,
 revision_text_bytes BIGINT,
 revision_text_bytes_diff BIGINT,
 revision_text_sha1 STRING,
 revision_content_model STRING,
 revision_content_format STRING,
 revision_is_deleted_by_page_deletion BOOLEAN,
 revision_deleted_by_page_deletion_timestamp STRING,
 revision_is_identity_reverted BOOLEAN,
 revision_first_identity_reverting_revision_id BIGINT,
 revision_seconds_to_identity_revert BIGINT,
 revision_is_identity_revert BOOLEAN,
 revision_is_from_before_page_creation BOOLEAN,
 revision_tags STRING
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY "\t"
  TBLPROPERTIES("skip.header.line.count"="1");


-- Create a table that is a bucketed version of pageviews
-- from October 20th on wiki title to increase query speed
CREATE TABLE IF NOT EXISTS WIKI_VIEWS_OCT_TITLE_BUCKET
  (DOMAIN STRING,
  WIKI_TITLE STRING,
  VIEWS INT,
  RESPONSE_SIZE INT)
  CLUSTERED BY (DOMAIN) INTO 20 BUCKETS
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY " "
  TBLPROPERTIES("skip.header.line.count"="1");

-- Create a table that is a bucketed version of pageviews
-- from September on wiki title to increase query speed
  CREATE TABLE IF NOT EXISTS WIKI_VIEWS_SEP_TITLE_BUCKET
    (DOMAIN STRING,
    WIKI_TITLE STRING,
    VIEWS INT,
    RESPONSE_SIZE INT)
    CLUSTERED BY (DOMAIN) INTO 20 BUCKETS
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY " "
    TBLPROPERTIES("skip.header.line.count"="1");


-- Create a table that partitions the wiki clickstream
-- based on link type to increase query speed
CREATE TABLE IF NOT EXISTS WIKI_CLICKSTREAM_LINK_PARTITION
  (WIKI_TITLE STRING,
  LINK_PAGE STRING,
  COUNT INT)
  PARTITIONED BY (LINK STRING)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY "\t"
  TBLPROPERTIES("skip.header.line.count"="1");

-- Create a table that helps group fields
-- from wiki edit by event global type
CREATE TABLE IF NOT EXISTS WIKI_EVENT_GLOBAL
  (
    wiki_db STRING,
 event_entity STRING,
 event_type STRING,
 event_timestamp STRING,
 event_comment STRING
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY "\t"
  TBLPROPERTIES("skip.header.line.count"="1");


-- Create a table that helps group fields
-- from wiki edit by event user type
CREATE TABLE IF NOT EXISTS WIKI_EVENT_USER
  (
    event_user_id BIGINT,
    event_user_text_historical STRING,
    event_user_text STRING,
    event_user_blocks_historical STRING,
    event_user_blocks STRING,
    event_user_groups_historical STRING,
    event_user_groups STRING,
    event_user_is_bot_by_historicalBIGINT STRING,
    event_user_is_bot_by STRING,
    event_user_is_created_by_self BOOLEAN,
    event_user_is_created_by_system BOOLEAN,
    event_user_is_created_by_peer BOOLEAN,
    event_user_is_anonymous BOOLEAN,
    event_user_registration_timestamp STRING,
    event_user_creation_timestamp STRING,
    event_user_first_edit_timestamp STRING,
    event_user_revision_count BIGINT,
    event_user_seconds_since_previous_revision BIGINT
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY "\t"
  TBLPROPERTIES("skip.header.line.count"="1");


-- Create a table that helps group fields
-- from wiki edit by page type
CREATE TABLE IF NOT EXISTS WIKI_PAGE
(
  page_id BIGINT,
  page_title_historical STRING,
  page_title STRING,
  page_namespace_historical INT,
  page_namespace_is_content_historical BOOLEAN,
  page_namespace INT,
  page_namespace_is_content BOOLEAN,
  page_is_redirect BOOLEAN,
  page_is_deleted BOOLEAN,
  page_creation_timestamp STRING,
  page_first_edit_timestamp STRING,
  page_revision_count BIGINT,
  page_seconds_since_previous_revision BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
TBLPROPERTIES("skip.header.line.count"="1");


-- Create a table that helps group fields
-- from wiki edit by user type
CREATE TABLE IF NOT EXISTS WIKI_USER
(
  user_id BIGINT,
  user_text_historical  STRING,
  user_text STRING,
  user_blocks_historical STRING,
  user_blocks STRING,
  user_groups_historical STRING,
  user_groups STRING,
  user_is_bot_by_historical STRING,
  user_is_bot_by STRING,
  user_is_created_by_self BOOLEAN,
  user_is_created_by_system BOOLEAN,
  user_is_created_by_peer BOOLEAN,
  user_is_anonymous BOOLEAN,
  user_registration_timestamp STRING,
  user_creation_timestamp STRING,
  user_first_edit_timestamp STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
TBLPROPERTIES("skip.header.line.count"="1");


-- Create a table that helps group fields
-- from wiki edit by revision type
CREATE TABLE IF NOT EXISTS WIKI_REVISION
  (
    revision_id BIGINT,
    revision_parent_id BIGINT,
    revision_minor_edit BOOLEAN,
    revision_deleted_parts STRING,
    revision_deleted_parts_are_suppressed BOOLEAN,
    revision_text_bytes BIGINT,
    revision_text_bytes_diff BIGINT,
    revision_text_sha1 STRING,
    revision_content_model STRING,
    revision_content_format STRING,
    revision_is_deleted_by_page_deletion BOOLEAN,
    revision_deleted_by_page_deletion_timestamp STRING,
    revision_is_identity_reverted BOOLEAN,
    revision_first_identity_reverting_revision_id BIGINT,
    revision_seconds_to_identity_revert BIGINT,
    revision_is_identity_revert BOOLEAN,
    revision_is_from_before_page_creation BOOLEAN,
    revision_tags STRING
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     TBLPROPERTIES("skip.header.line.count"="1");

-- Load all data from HDFS into their respective tables
LOAD DATA INPATH '/user/srtidd/inputs/october-20-pageviews' INTO TABLE WIKI_VIEWS_OCT;
LOAD DATA INPATH '/user/srtidd/inputs/september-all-pageviews' INTO TABLE WIKI_VIEWS_SEP;
LOAD DATA INPATH '/user/srtidd/inputs/clickstream-enwiki-2020-09.tsv' INTO TABLE WIKI_CLICKSTREAM;
LOAD DATA INPATH '/user/srtidd/inputs/2020-09.enwiki.2020-10.tsv' INTO TABLE WIKI_EDITS;

-- Copy all data from core external tables into bucket, partitioned,
-- and sub tables
INSERT INTO WIKI_VIEWS_OCT_TITLE_BUCKET
SELECT DOMAIN, WIKI_TITLE, VIEWS, RESPONSE_SIZE FROM WIKI_VIEWS_OCT;

INSERT INTO WIKI_VIEWS_SEP_TITLE_BUCKET
SELECT DOMAIN, WIKI_TITLE, VIEWS, RESPONSE_SIZE FROM WIKI_VIEWS_SEP;


INSERT INTO WIKI_CLICKSTREAM_LINK_PARTITION PARTITION(LINK)
SELECT WIKI_TITLE, LINK_PAGE, COUNT, LINK FROM WIKI_CLICKSTREAM;

INSERT INTO WIKI_EVENT_GLOBAL
SELECT wiki_db, event_entity, event_type, event_timestamp, event_comment FROM WIKI_EDITS;

INSERT INTO WIKI_EVENT_USER
SELECT event_user_id, event_user_text_historical, event_user_text, event_user_blocks_historical,
 event_user_blocks, event_user_groups_historical, event_user_groups, event_user_is_bot_by_historical,
 event_user_is_bot_by, event_user_is_created_by_self, event_user_is_created_by_system,
 event_user_is_created_by_peer, event_user_is_anonymous, event_user_registration_timestamp,
 event_user_creation_timestamp, event_user_first_edit_timestamp, event_user_revision_count,
 event_user_seconds_since_previous_revision FROM WIKI_EDITS;

INSERT INTO WIKI_PAGE
SELECT page_id, page_title_historical, page_title, page_namespace_historical, page_namespace_is_content_historical, page_namespace,
 page_namespace_is_content, page_is_redirect, page_is_deleted, page_creation_timestamp, page_first_edit_timestamp,
 page_revision_count, page_seconds_since_previous_revision FROM WIKI_EDITS;

INSERT INTO WIKI_USER
SELECT user_id, user_text_historical, user_text, user_blocks_historical, user_blocks, user_groups_historical,
  user_groups, user_is_bot_by_historical, user_is_bot_by, user_is_created_by_self, user_is_created_by_system,
  user_is_created_by_peer, user_is_anonymous, user_registration_timestamp, user_creation_timestamp, user_first_edit_timestamp
  FROM WIKI_EDITS;

INSERT INTO WIKI_REVISION
SELECT   revision_id, revision_parent_id, revision_minor_edit, revision_deleted_parts, revision_deleted_parts_are_suppressed,
  revision_text_bytes, revision_text_bytes_diff, revision_text_sha1, revision_content_model, revision_content_format,
  revision_is_deleted_by_page_deletion, revision_deleted_by_page_deletion_timestamp, revision_is_identity_reverted,
  revision_first_identity_reverting_revision_id, revision_seconds_to_identity_revert, revision_is_identity_revert,
  revision_is_from_before_page_creation, revision_tags FROM WIKI_EDITS;
