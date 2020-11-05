--Show the articles that are more popular in one
-- country compared to others. Popularity in this case is the highest
-- frequency of edits




--UK according to timezone and internet rush hour
CREATE TABLE UK_EDIT_POPULARITY AS
SELECT PAGE_TITLE, "UK" AS REGION, COUNT(PAGE_TITLE) AS EDIT_POPULARITY
FROM WIKI_EDITS
WHERE event_entity != "user" AND PAGE_TITLE != "" AND(
      event_timestamp LIKE ("___________19:%") OR
      event_timestamp LIKE ("___________20:%") OR
      event_timestamp LIKE ("___________21:%") OR
      event_timestamp LIKE ("___________22:%") OR
      event_timestamp LIKE ("___________23:%"))
GROUP BY PAGE_TITLE;

SELECT * FROM UK_EDIT_POPULARITY
LIMIT 100;

--US according to timezone and internet rush hour
CREATE TABLE US_EDIT_POPULARITY AS
SELECT PAGE_TITLE, "US" AS REGION, COUNT(PAGE_TITLE) AS EDIT_POPULARITY
FROM WIKI_EDITS
WHERE  event_entity != "user" AND PAGE_TITLE != "" AND
      (event_timestamp LIKE ("___________11:%") OR
      event_timestamp LIKE ("___________12:%") OR
      event_timestamp LIKE ("___________13:%") OR
      event_timestamp LIKE ("___________14:%") OR
      event_timestamp LIKE ("___________15:%") OR
      event_timestamp LIKE ("___________16:%") OR
      event_timestamp LIKE ("___________17:%") OR
      event_timestamp LIKE ("___________18:%"))
GROUP BY PAGE_TITLE;

--AU according to timezone and internet rush hour
CREATE TABLE AU_EDIT_POPULARITY AS
SELECT PAGE_TITLE, "AU" AS REGION, COUNT(PAGE_TITLE) AS EDIT_POPULARITY
FROM WIKI_EDITS
WHERE event_entity != "user" AND PAGE_TITLE != "" AND
      (event_timestamp LIKE ("___________03:%") OR
      event_timestamp LIKE ("___________04:%") OR
      event_timestamp LIKE ("___________05:%") OR
      event_timestamp LIKE ("___________06:%") OR
      event_timestamp LIKE ("___________07:%") OR
      event_timestamp LIKE ("___________08:%") OR
      event_timestamp LIKE ("___________09:%"))
GROUP BY PAGE_TITLE;

-- Table that contains all 3 countries in terms of edit popularity
CREATE TABLE COUNTRY_EDIT_POPULARITY
(
  PAGE_TITLE STRING,
  EDIT_POPULARITY INT
)
PARTITIONED BY(REGION STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t";

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE COUNTRY_EDIT_POPULARITY PARTITION(REGION)
SELECT PAGE_TITLE, EDIT_POPULARITY, REGION FROM UK_EDIT_POPULARITY;

INSERT INTO TABLE COUNTRY_EDIT_POPULARITY PARTITION(REGION)
SELECT PAGE_TITLE, EDIT_POPULARITY, REGION FROM US_EDIT_POPULARITY;

INSERT INTO TABLE COUNTRY_EDIT_POPULARITY PARTITION(REGION)
SELECT PAGE_TITLE, EDIT_POPULARITY, REGION FROM AU_EDIT_POPULARITY;

--Table with all the articles that are edited more by a
-- specific country than others
CREATE TABLE FINAL_EDIT_POPULARITY AS
SELECT COUNTRY.PAGE_TITLE, COUNTRY.REGION, MAXEDIT.EDIT_POPULARITY
FROM COUNTRY_EDIT_POPULARITY AS COUNTRY
INNER JOIN (
    SELECT PAGE_TITLE, MAX(EDIT_POPULARITY) AS EDIT_POPULARITY
    FROM COUNTRY_EDIT_POPULARITY
    GROUP BY PAGE_TITLE
) AS MAXEDIT ON (MAXEDIT.PAGE_TITLE = COUNTRY.PAGE_TITLE AND MAXEDIT.EDIT_POPULARITY = COUNTRY.EDIT_POPULARITY);


-- Queries for final results
-- Most edit popular in UK
SELECT * FROM FINAL_EDIT_POPULARITY
WHERE REGION = "UK"
ORDER BY EDIT_POPULARITY DESC
LIMIT 3;

-- Most edit popular in US
SELECT * FROM FINAL_EDIT_POPULARITY
WHERE REGION = "US"
ORDER BY EDIT_POPULARITY DESC
LIMIT 3;

-- Most edit popular in AU
SELECT * FROM FINAL_EDIT_POPULARITY
WHERE REGION = "AU"
ORDER BY EDIT_POPULARITY DESC
LIMIT 3;
