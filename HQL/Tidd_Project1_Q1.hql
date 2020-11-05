-- Return top 10 most viewed Wiki articles

CREATE DATABASE WIKIDB;

USE WIKIDB;

--  Table with the total views of
-- each article in the month of September 2020
CREATE EXTERNAL TABLE IF NOT EXISTS WIKI_SEP_TOTAL_VIEWS
(
  WIKI_TITLE STRING,
  TOTAL_VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t";

-- Table with the total views of
-- each article on the date of October 20th
CREATE  EXTERNAL TABLE IF NOT EXISTS WIKI_OCT_TOTAL_VIEWS
(
  WIKI_TITLE STRING,
  TOTAL_VIEWS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t";

LOAD DATA INPATH '/user/srtidd/outputs/october-views' INTO TABLE WIKI_OCT_TOTAL_VIEWS;
LOAD DATA INPATH '/user/srtidd/outputs/september-views' INTO TABLE WIKI_SEP_TOTAL_VIEWS;


-- Query for final results
SELECT * from WIKI_OCT_TOTAL_VIEWS
ORDER BY TOTAL_VIEWS DESC
limit 10;
