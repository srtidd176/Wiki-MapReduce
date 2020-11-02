-- The series with the largest fraction of viewers
-- clicking on an internal link


-- Create a table that has the most popular sublink of a wiki page
CREATE TABLE PAGE_SUB_LINKS
(
  WIKI_TITLE STRING,
  LINK STRING,
  NUM_LINKS INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"

LOAD DATA INPATH "/user/srtidd/outputs/page_sub_links" INTO TABLE PAGE_SUB_LINKS;


CREATE TABLE SERIES_PATH
(
  WIKI_TITLE STRING,
  FRACTION INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY " ";
