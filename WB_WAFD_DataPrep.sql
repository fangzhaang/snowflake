USE ROLE sde_candidate_role;
USE DATABASE open_test;
USE SCHEMA public_test;
USE WAREHOUSE opentest_public_vwh;


--Create file format
CREATE OR REPLACE FILE FORMAT SPOKENLANGUAGES
     TYPE = CSV
     COMPRESSION = GZIP
     FIELD_DELIMITER = ','
     SKIP_HEADER = 1
     FIELD_OPTIONALLY_ENCLOSED_BY = '"';

--Create local staging environment
CREATE OR REPLACE STAGE RAW_DATA_STAGING;


--Load .csv to staging environment using SNOWSQL CLI Client
PUT file://D:\WaFd\sde_spokenlanguages_data.csv @RAW_DATA_STAGING;

--Confirm file loaded to RAW_DATA_STAGING
LIST @RAW_DATA_STAGING;

--Create stage table for raw language data
CREATE OR REPLACE TABLE "OPEN_TEST"."PUBLIC_TEST"."RAW_SPOKENLANGUAGES_DATA"(
	data_type varchar(50),
	Arizona varchar(50),
	Idaho varchar(50),
	Nevada varchar(50),
	"New Mexico" varchar(50),
	Oregon varchar(50),
	Texas varchar(50),
	Utah varchar(50),
	Washington varchar(50)
	
);

--Load raw_language_data from stage
COPY INTO "OPEN_TEST"."PUBLIC_TEST"."RAW_SPOKENLANGUAGES_DATA"
     FROM '@RAW_DATA_STAGING'
     FILE_FORMAT = SPOKENLANGUAGES
     
--Validate data loaded to table
SELECT * FROM "OPEN_TEST"."PUBLIC_TEST"."RAW_SPOKENLANGUAGES_DATA"


--Load STG_SPOKEN_LANGUAGES from RAW_SPOKENLANGUAGES_DATA
CREATE OR REPLACE TABLE "OPEN_TEST"."PUBLIC_TEST"."STG_SPOKEN_LANGUAGES"(
STATE varchar(50)
,LANGUAGES varchar(150)
,VALUE bigint
)
  AS SELECT 
Statename 
,Spoken
,Stat

FROM (SELECT data_type AS spoken
,REPLACE(STAT,',','') as stat
,STATENAME
FROM "OPEN_TEST"."PUBLIC_TEST"."RAW_SPOKENLANGUAGES_DATA" 
UNPIVOT
(
  STAT
  for STATENAME in(Arizona,Idaho, Utah, Washington, Oregon, Nevada, Texas, "New Mexico")
) unpiv)

--Validate stg_spoken_languages
SELECT * from stg_spoken_languages