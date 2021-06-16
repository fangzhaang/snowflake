USE ROLE sde_candidate_role;
USE DATABASE open_test;
USE SCHEMA public_test;
USE WAREHOUSE opentest_public_vwh;

--create table to store WaFd locations
	--create sequence id
CREATE OR REPLACE SEQUENCE seq_wafdlocation;

CREATE OR REPLACE TABLE wafd_location
(location_id integer default seq_wafdlocation.nextval
 ,state varchar(50)
);

	--insert data to table
INSERT INTO wafd_location (state)
values ('WA')
    ,('OR')
    ,('ID')
    ,('NV')
    ,('UT')
    ,('AZ')
    ,('NM')
    ,('TX');
    
   --check
SELECT * FROM wafd_location;

CREATE OR REPLACE TABLE stg_state_households_data
(state varchar(50)
,data_type varchar(150)
,stats bigint
);



