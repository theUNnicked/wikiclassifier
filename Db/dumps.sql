SET SCHEMA WIKICLASSIFIER;
DELETE FROM CATEGORYLINKS;
RUNSCRIPT FROM '/macierz/home/143365sk/KAW/matrixudump/cli2.sql';

SET SCHEMA WIKICLASSIFIER;
DELETE FROM PAGE;
RUNSCRIPT FROM '/macierz/home/143365sk/KAW/matrixudump/pgi3.sql';

SET SCHEMA wikiclassifier;
DROP VIEW IF EXISTS v_category_page;
CREATE VIEW WIKICLASSIFIER.V_CATEGORY_PAGE AS
SELECT
    CL.CL_TO,
    PG.PAGE_TITLE
FROM WIKICLASSIFIER.CATEGORYLINKS CL
JOIN WIKICLASSIFIER.PAGE PG
ON CL.CL_FROM = PG.PAGE_ID;