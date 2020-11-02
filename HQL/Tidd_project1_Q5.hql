
-- Create a table that selects wiki articles that
-- revert previous revisions (assuming these are corrections
-- to vandalism)

CREATE TABLE WIKI_VANDALS AS
SELECT page_title_historical, revision_seconds_to_identity_revert
FROM WIKI_EDITS
WHERE WIKI_EDITS.wiki_db == "enwiki";
 
