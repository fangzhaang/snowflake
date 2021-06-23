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


USE ROLE sde_candidate_role;
USE DATABASE open_test;
USE SCHEMA public_test;
USE WAREHOUSE opentest_public_vwh;

CREATE OR REPLACE TABLE WAFD_LOCATION
(location_id integer default seq_wafdlocation.nextval
 ,state varchar(50)
 ,state_abbr varchar(2)
);

	--insert data to table
INSERT INTO wafd_location (state,state_abbr)
values ('Washington','WA')
    ,('Oregon','OR')
    ,('Idaho','ID')
    ,('Nevada','NV')
    ,('Utah','UT')
    ,('Arizona','AZ')
    ,('New Mexico','NM')
    ,('Texas','TX');
    
    
--Create sequence for language_Dim
CREATE OR REPLACE SEQUENCE seq_language_dim
  START WITH = 1000
  INCREMENT BY =  1;
    
--Create & load language_dim
CREATE OR REPLACE TABLE LANGUAGE_DIM(
  LANGUAGE_ID BIGINT
  ,LOCATION_ID BIGINT
  ,SPANISH_ONLY INTEGER
  ,SPANISH_BILINGUAL INTEGER
  ,OTHER_INDO_EUROPEAN_LANG_BILINGUAL INTEGER
  ,OTHER_INDO_EUROPEAN_LANG_ONLY INTEGER
  ,ASIAN_LANG_BILINGUAL INTEGER
  ,ASIAN_LANG_ONLY INTEGER
  ,OTHER_LANG_BILINGUAL INTEGER
  ,OTHER_LANG_ONLY INTEGER
)
AS
SELECT
     seq_language_dim.nextval
     ,locid
    ,"'Spanish Only'"
    ,"'Spanish Bilingual'"
    ,"'Other Indo-European Languages Bilingual'"
    ,"'Other Indo-European Languages Only'"
    ,"'Asian Languages Bilingual'"
    ,"'Asian Languages Only'"
    ,"'Other Languages Bilingual'"
    ,"'Other Languages Only'"
FROM (

SELECT  STG.state, languages, value, wl.location_id as locid
  FROM "OPEN_TEST"."PUBLIC_TEST"."STG_SPOKEN_LANGUAGES" stg
inner join wafd_location wl on UPPER(stg.State) = UPPER(wl.state))
 AS SourceTable  
PIVOT  
(  
  MAX(value)  
  FOR languages IN (
    'Spanish Only'
    ,'Spanish Bilingual'
    ,'Other Indo-European Languages Bilingual'
    ,'Other Indo-European Languages Only'
    ,'Asian Languages Bilingual'
    ,'Asian Languages Only'
    ,'Other Languages Bilingual'
    ,'Other Languages Only')  
) AS Pivot;  

--validate language_dim
SELECT * FROM LANGUAGE_DIM
    
--Create sequence for Demographic_fact
CREATE OR REPLACE SEQUENCE seq_demographic_fact
  START WITH = 1000
  INCREMENT BY =  1;
    
--Create & Load Demographic Fact
CREATE OR REPLACE TABLE DEMOGRAPHIC_FACT
(demographic_id bigint
 ,LOCATION_ID bigint
 ,HOUSEHOLDS_TOTAL integer
 ,HOUSEHOLDS_WITHOUT_PC integer
 ,HOUSEHOLDS_WITH_PC integer
 ,HIGHSCHOOL_GRAD_OR_HIGHER integer
 ,ASSOSCIATE_DEGREE integer 
 ,BACHELOR_DEGREE integer
 ,POPULATION_25_UP integer
 ,TOTAL_POPULATION integer
)
    AS 
    SELECT 
    seq_demographic_fact.nextval
    ,locid 
    ,"'Total housholds'" 
    ,"'Households without a computer'"
    ,"'Households with a computer'"
    ,"'Associate degree'"
    ,"'Bachelor degree and higher'"
    ,"'High school graduate and higher'"
    ,"'Population 25 years and over'"
    ,"'Total population'"
  
FROM  
(
  SELECT stg.state, data_type, stats, wl.location_id as locid
  FROM "OPEN_TEST"."PUBLIC_TEST"."STG_STATE_HOUSEHOLDS_DATA" stg
inner join wafd_location wl on stg.State = wl.state)
 AS SourceTable  
PIVOT  
(  
  MAX(stats)  
  FOR data_type IN (
    'Total housholds'
    ,'Households without a computer'
    ,'Households with a computer'
    ,'Bachelor degree and higher'
    ,'High school graduate and higher'
    ,'Associate degree'
    ,'Population 25 years and over'
    ,'Total population')  
) AS Pivot;  

--Validate demographic_fact
select * from demographic_fact


--Create Customer View
CREATE OR REPLACE VIEW "OPEN_TEST"."PUBLIC_TEST"."Customer_View" (
LOCATION_ID
,STATE_CODE
,AVG_PERSONS_PER_HOUSEHOLD
,PERCENT_HAVE_COMPUTER
,PERCENT_HAVE_HIGHSCHOOL_DEGREE_OR_HIGHER
,PERCENT_ENGLISH_ONLY
,PERCENT_BILINGUAL
,PERCENT_NO_ENGLISH
)
AS
SELECT

DF.LOCATION_ID
,WL.STATE_ABBR
,(TOTAL_POPULATION/HOUSEHOLDS_TOTAL)
,(HOUSEHOLDS_WITH_PC/HOUSEHOLDS_TOTAL)*100 
,(HIGHSCHOOL_GRAD_OR_HIGHER/TOTAL_POPULATION)*100  --Include Assoc, bachelors??
,'NO_DATA'  --no data on english only, don't want to assume w/o further data
,((SPANISH_BILINGUAL+OTHER_INDO_EUROPEAN_LANG_BILINGUAL+ASIAN_LANG_BILINGUAL+OTHER_LANG_BILINGUAL)/TOTAL_POPULATION)*100
,((SPANISH_ONLY+OTHER_INDO_EUROPEAN_LANG_ONLY+ASIAN_LANG_ONLY+OTHER_LANG_ONLY)/TOTAL_POPULATION)*100

FROM DEMOGRAPHIC_FACT DF                        
INNER JOIN WAFD_LOCATION WL ON DF.LOCATION_ID = WL.LOCATION_ID             
INNER JOIN LANGUAGE_DIM LD ON DF.LOCATION_ID = LD.LOCATION_ID


--Validate Customer_View
select * from "OPEN_TEST"."PUBLIC_TEST"."Customer_View"           