-- The series with the largest fraction of viewers
-- clicking on an internal link


-- Create a table that has the most popular sublink of a wiki page
CREATE EXTERNAL TABLE IF NOT EXISTS WIKI_MAX_LINK
(
  WIKI_TITLE STRING,
  MAX_LINK STRING,
  NUM_LINKS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t";

LOAD DATA INPATH "/user/srtidd/outputs/wiki-max-link" INTO TABLE WIKI_MAX_LINK;

-- Create a table that has the fraction of viewers that click
-- on its most popular link
CREATE TABLE IF NOT EXISTS MAX_LINK_FRACTION AS
SELECT WIKI_MAX_LINK.WIKI_TITLE, MAX_LINK, (NUM_LINKS/TOTAL_VIEWS) AS PERCENTAGE
FROM WIKI_MAX_LINK JOIN WIKI_SEP_TOTAL_VIEWS
ON (WIKI_MAX_LINK.WIKI_TITLE = WIKI_SEP_TOTAL_VIEWS.WIKI_TITLE)
WHERE NUM_LINKS < TOTAL_VIEWS;

-- Create a table to hold the path of the series starting from Hotel_California
CREATE TABLE SERIES_PATH
(
  WIKI_TITLE STRING,
  LINK STRING,
  FRACTION FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY " ";

-- begin by insterting Hotel_California as the first entry
INSERT INTO SERIES_PATH
SELECT WIKI_TITLE, MAX_LINK, PERCENTAGE
FROM MAX_LINK_FRACTION
WHERE WIKI_TITLE = "Eagles_Greatest_Hits,_Vol._2";

-- Query for final results
SELECT * FROM SERIES_PATH;
