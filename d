CREATE OR REPLACE PROCEDURE CKF_PRD_STARS_DS_DB.DATASENTINEL_COMPACT.SP_DSE_ENGINE("DSID" VARCHAR(16777216), "RUNID" VARCHAR(16777216), "SRC_TMP" VARCHAR(16777216), "TGT_TMP" VARCHAR(16777216), "ENVIRONMENT_ID" NUMBER(38,0))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'declare 

--Declaring the variables 

v_TestType                                                   VARCHAR;

v_Criticality                                     VARCHAR;

v_JobDescription                          VARCHAR;

v_ResultType                                 VARCHAR;

v_RUNDATE                                                  DATETIME;

v_StartTime                                                  DATETIME; 

v_DSStartTime                               TIMESTAMP;

v_DSEndTime                                               TIMESTAMP;

v_Rownr                                                                      INT;

v_Targettable                                 VARCHAR;

v_frequencycheck VARCHAR;

v_TestCaseDescription   VARCHAR;

v_RESULTSETCOLS         VARCHAR;

v_returncode                                 INT;

v_JobName                                                   VARCHAR;

v_RPT_MO_KEY                             INT;

v_RPT_MO_KEY_PREV                 INT;

v_STEP                                                           VARCHAR;

v_RunId              VARCHAR;

v_output            VARCHAR;

v_Dynamic_TargetTable   VARCHAR;

v_colstr               VARCHAR;

v_Tab_TestCaseList        VARCHAR;

v_Tab_MetricList            VARCHAR;

v_Tab_Insert_MetricList VARCHAR;

v_drop_MetricList          VARCHAR;

v_text                  VARCHAR;

v_update                          VARCHAR;

v_col_Cnt            INTEGER;

v_res_failed                                   VARCHAR;          

v_metric_name           VARCHAR;

v_metric_name_error     VARCHAR;

v_res_col1              VARCHAR;

v_res_col2              VARCHAR;

v_res_col3              VARCHAR;

v_res_col4              VARCHAR;

v_res_col5              VARCHAR;

v_res_col6              VARCHAR;

v_res_col7              VARCHAR;

v_res_col8              VARCHAR;

v_res_col9              VARCHAR;

v_res_col10             VARCHAR;

v_ENVIRONMENT_ID             int;

v_thresholdId			varchar;

v_thresholdtype 		varchar;

v_minwarningthreshold	varchar;

v_maxwarningthreshold	varchar;

v_minfailthreshold		varchar;

v_maxfailthreshold		varchar;

v_ceilingvalue			varchar;

v_floorvalue			varchar;

v_JOBNAME_DSE_TESTRESULTS VARCHAR; 

v_EXECUTIONSTEP_DSE_TESTRESULTS VARCHAR ;

v_TESTCASEDESCRIPTION_DSE_TESTRESULTS VARCHAR;

v_FREQUENCYCOUNTER_DSE_TESTRESULTS VARCHAR;

v_RUNDATE_DSE_TESTRESULTS VARCHAR;

v_INSERTTIMESTAMP_DSE_TESTRESULTS VARCHAR;

v_Runid_DSE_TESTRESULTS VARCHAR;

v_ACTV_IND_DSE_TESTRESULTS VARCHAR;  

v_TOTALRUNTIME_DSE_TESTRESULTS VARCHAR; 

v_SQLRUNTIME_DSE_TESTRESULTS VARCHAR; 

v_MAX_RUNID VARCHAR;





BEGIN



--set session timezone to CST

--alter session set timestamp_type_mapping = ''TIMESTAMP_LTZ'';

--alter session set timezone = ''America/Chicago'';



--Initialization of Variables 

v_StartTime :=sysdate();

v_Rundate   :=sysdate();

select sysdate(),sysdate() into :v_DSStartTime,:v_Rundate;

v_res_failed :='''';

v_RowNr:=1;

v_RunId := :RunId;

v_col_Cnt := 0;

v_ENVIRONMENT_ID := :ENVIRONMENT_ID;







     SELECT distinct TP.Criticality,TP.Targettable,JC.JobName,TestType,ResultType,TP.ExecutionStep,TestCaseDescription,NVL(frequencycheck,0)

                                               into 

        :v_Criticality,:v_Targettable,:v_JobName,:v_TestType,:v_ResultType,:v_Step,:v_TestCaseDescription,:v_frequencycheck

                                           FROM DATASENTINEL_COMPACT.DSE_TESTPLAN TP

                                           inner join datasentinel_compact.dse_job_config JC on 

                                           JC.jobid=tp.jobid

                                           WHERE DSID=:DSID;

           

     SELECT distinct case when testtype = ''SINGLE DATAPOINT'' then RESULTSETCOLS ELSE RESULTSETCOLS-1 END  into :v_RESULTSETCOLS FROM DSE_TestPlan WHERE DSID = :DSID;



if (upper(:v_Targettable)=''DSE_TESTRESULTS'') then  

                                           

BEGIN


--Capture column count into a variable to handle single data point vs multi data point
   

      v_Dynamic_TargetTable := ''Create or Replace  temporary Table TEMP_MDP_''||regexp_replace(:v_JobName,''[^a-zA-Z0-9]'', ''_'')||''_''||:DSID||'' as Select * from ''||:Tgt_tmp;



  execute immediate :v_Dynamic_TargetTable; 





--Capture column count into a variable to handle single data point vs multi data point

let v_table_name varchar(200):=upper(''TEMP_MDP_''|| regexp_replace(:v_JobName,''[^a-zA-Z0-9]'', ''_'')||''_''||:DSID);



    

  --  let v_table_name varchar=:Tgt_tmp;

    

select count(*) into :v_col_cnt from information_schema.columns where table_name=:v_table_name;

select column_name into :v_metric_name from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 1;

select column_name into :v_res_col1 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 2;

select column_name into :v_res_col2 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 3;

select column_name into :v_res_col3 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 4;

select column_name into :v_res_col4 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 5;

select column_name into :v_res_col5 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 6;

select column_name into :v_res_col6 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 7;

select column_name into :v_res_col7 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 8;

select column_name into :v_res_col8 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 9;

select column_name into :v_res_col9 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 10;

select column_name into :v_res_col10 from information_schema.columns where table_name=:v_table_name and ORDINAL_POSITION = 11;





    create or replace temporary table TEMP_DSE_DataProfile_MetricList

            (

                DSID number(38,0), 

                JobName varchar,

                executionStep varchar,

                ENVIRONMENT_ID varchar,

                TestCaseDescription varchar,

                MetricId number(38,0),

                MetricName varchar,

                Result1 varchar,

                PriorResult1 varchar,

                Delta1 varchar, 

                PercentChange1 varchar,

                TestStatus1 varchar,       

                Result2 varchar,

                PriorResult2 varchar,

                Delta2 varchar, 

                PercentChange2 varchar,

                TestStatus2 varchar,   

                Result3 varchar,

                PriorResult3 varchar,

                Delta3 varchar, 

                PercentChange3 varchar, 

                TestStatus3 varchar, 

                Result4 varchar,

                PriorResult4 varchar,

                Delta4 varchar, 

                PercentChange4 varchar, 

                TestStatus4 varchar,       

                Result5 varchar,

                PriorResult5 varchar,

                Delta5 varchar, 

                PercentChange5 varchar, 

                TestStatus5 varchar,  

                Result6 varchar,

                PriorResult6 varchar,

                Delta6 varchar, 

                PercentChange6 varchar, 

                TestStatus6 varchar,    

                Result7 varchar,

                PriorResult7 varchar,

                Delta7 varchar, 

                PercentChange7 varchar, 

                TestStatus7 varchar, 

                Result8 varchar,

                PriorResult8 varchar,

                Delta8 varchar, 

                PercentChange8 varchar, 

                TestStatus8 varchar,      

                Result9 varchar,

                PriorResult9 varchar,

                Delta9 varchar, 

                PercentChange9 varchar, 

                TestStatus9 varchar,    

                Result10 varchar,

                PriorResult10 varchar,

                Delta10 varchar, 

                PercentChange10 varchar, 

                TestStatus10 varchar,                     

                FrequencyCounter  number(38,0), 

                rowNr integer,

                RunID number(38,0)

            );              

        

        BEGIN

            v_Tab_Insert_MetricList :=''insert into TEMP_DSE_DataProfile_MetricList( DSID,Jobname,ExecutionStep,ENVIRONMENT_ID, 

            MetricName,Result1,Result2,Result3,Result4,Result5,Result6,Result7,Result8,Result9,Result10) 

           select ''||:DSID||'',''''''||:v_JobName||'''''',

           ''''''||:v_Step||'''''',''''''||:v_ENVIRONMENT_ID||'''''',

           ''||:v_metric_name||'',

           	   IFF(try_to_number(''||:v_res_col1||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col1||'')::varchar,iff(''||:v_res_col1||''=''''nan'''','''''''',''||:v_res_col1||'')),

		   IFF(try_to_number(''||:v_res_col2||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col2||'')::varchar,iff(''||:v_res_col2||''=''''nan'''','''''''',''||:v_res_col2||'')),

		   IFF(try_to_number(''||:v_res_col3||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col3||'')::varchar,iff(''||:v_res_col3||''=''''nan'''','''''''',''||:v_res_col3||'')),

		   IFF(try_to_number(''||:v_res_col4||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col4||'')::varchar,iff(''||:v_res_col4||''=''''nan'''','''''''',''||:v_res_col4||'')),

		   IFF(try_to_number(''||:v_res_col5||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col5||'')::varchar,iff(''||:v_res_col5||''=''''nan'''','''''''',''||:v_res_col5||'')),

		   IFF(try_to_number(''||:v_res_col6||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col6||'')::varchar,iff(''||:v_res_col6||''=''''nan'''','''''''',''||:v_res_col6||'')),

		   IFF(try_to_number(''||:v_res_col7||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col7||'')::varchar,iff(''||:v_res_col7||''=''''nan'''','''''''',''||:v_res_col7||'')),

		   IFF(try_to_number(''||:v_res_col8||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col8||'')::varchar,iff(''||:v_res_col8||''=''''nan'''','''''''',''||:v_res_col8||'')),

		   IFF(try_to_number(''||:v_res_col9||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col9||'')::varchar,iff(''||:v_res_col9||''=''''nan'''','''''''',''||:v_res_col9||'')),

		   IFF(try_to_number(''||:v_res_col10||'')::varchar IS NOT NULL,try_to_number(''||:v_res_col10||'')::varchar,iff(''||:v_res_col10||''=''''nan'''','''''''',''||:v_res_col10||''))

           

FROM TEMP_MDP_''||regexp_replace(:v_JobName,''[^a-zA-Z0-9]'', ''_'')||''_''||:DSID;

        

            execute immediate :v_Tab_Insert_MetricList;        

          

            EXCEPTION

               when statement_error then

               v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

               execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No 

               Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

               return :v_res_failed;

            

               when expression_error then 

               v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

               execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No 

               Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

               return :v_res_failed;

            

               when other then 

               v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

               execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No 

               Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

               return :v_res_failed;



        END;





        create or replace temporary table TEMP_DSE_TESTRESULTS_STG

            (

                DSID number(38,0), 

                JobName varchar,

                executionStep varchar,

                ENVIRONMENT_ID varchar,

                TestCaseDescription varchar,

                MetricId number(38,0),

                MetricName varchar,

                Result1 varchar,

                PriorResult1 varchar,

                Delta1 varchar, 

                PercentChange1 varchar,

                TestStatus1 varchar,       

                Result2 varchar,

                PriorResult2 varchar,

                Delta2 varchar, 

                PercentChange2 varchar,

                TestStatus2 varchar,   

                Result3 varchar,

                PriorResult3 varchar,

                Delta3 varchar, 

                PercentChange3 varchar, 

                TestStatus3 varchar, 

                Result4 varchar,

                PriorResult4 varchar,

                Delta4 varchar, 

                PercentChange4 varchar,

                TestStatus4 varchar,  

                Result5 varchar,

                PriorResult5 varchar,

                Delta5 varchar, 

                PercentChange5 varchar,

                TestStatus5 varchar,                   

                Result6 varchar,

                PriorResult6 varchar,

                Delta6 varchar, 

                PercentChange6 varchar,

                TestStatus6 varchar,                   

                Result7 varchar,

                PriorResult7 varchar,

                Delta7 varchar, 

                PercentChange7 varchar,

                TestStatus7 varchar,                   

                Result8 varchar,

                PriorResult8 varchar,

                Delta8 varchar, 

                PercentChange8 varchar,

                TestStatus8 varchar,     

                Result9 varchar,

                PriorResult9 varchar,

                Delta9 varchar, 

                PercentChange9 varchar,

                TestStatus9 varchar,       

                Result10 varchar,

                PriorResult10 varchar,

                Delta10 varchar, 

                PercentChange10 varchar,

                TestStatus10 varchar,                     

                FrequencyCounter number(38,0), 

                ACTV_IND varchar,

                RunID number(38,0),

                RUNDATE TIMESTAMP,

                INSERTTIMESTAMP TIMESTAMP,

                RPT_MO_KEY number(38,0), 

                TOTALRUNTIME number(38,0), 

                SQLRUNTIME number(38,0)

            ); 

            

                    

            --Capture MAX RUNID into a variable

            BEGIN

            select MAX(RUNID) into :v_MAX_RUNID from DSE_TESTRESULTS where DSID = :DSID and ACTV_IND = ''Y'' and ENVIRONMENT_ID = :v_ENVIRONMENT_ID;

                                           EXCEPTION

                                           WHEN OTHER THEN

                                           v_MAX_RUNID:=NULL;

                                           END;

            

                                  

            create or replace temporary table tmp_fltn as (

            select p.JOBNAME,p.EXECUTIONSTEP,p.ENVIRONMENT_ID,p.TESTCASEDESCRIPTION,NVL(p.FrequencyCounter,0) as FrequencyCounter ,p.rundate,p.insertTimestamp,p.RUNID,p.ACTV_IND,

                p.TOTALRUNTIME,p.SQLRUNTIME,f1.seq, f1.key, f1.value::varchar as rslt 

                from DSE_TESTRESULTS p,

                lateral flatten(input => p.METRICRESULTS) f,

                lateral flatten(input => f.value) f1 

                where p.dsid = :DSID 

                and p.ENVIRONMENT_ID = :v_ENVIRONMENT_ID);           

           

            insert into TEMP_DSE_TESTRESULTS_STG(DSID,JOBNAME,EXECUTIONSTEP,ENVIRONMENT_ID,TESTCASEDESCRIPTION,FrequencyCounter,rundate,insertTimestamp,RUNID,ACTV_IND,TOTALRUNTIME,SQLRUNTIME,METRICID,

            METRICNAME,RESULT1,PRIORRESULT1,DELTA1,PERCENTCHANGE1,

            TESTSTATUS1,RESULT2,PRIORRESULT2,DELTA2,PERCENTCHANGE2,TESTSTATUS2, 

            RESULT3,PRIORRESULT3,DELTA3,PERCENTCHANGE3,TESTSTATUS3,RESULT4,PRIORRESULT4,DELTA4,PERCENTCHANGE4,TESTSTATUS4,

            RESULT5,PRIORRESULT5,DELTA5,PERCENTCHANGE5,TESTSTATUS5,

            RESULT6,PRIORRESULT6,DELTA6,PERCENTCHANGE6,TESTSTATUS6,

            RESULT7,PRIORRESULT7,DELTA7,PERCENTCHANGE7,TESTSTATUS7,

            RESULT8,PRIORRESULT8,DELTA8,PERCENTCHANGE8,TESTSTATUS8,

            RESULT9,PRIORRESULT9,DELTA9,PERCENTCHANGE9,TESTSTATUS9,

            RESULT10,PRIORRESULT10,DELTA10,PERCENTCHANGE10,TESTSTATUS10)                  

            select :DSID,JOBNAME,EXECUTIONSTEP,ENVIRONMENT_ID, TESTCASEDESCRIPTION,FrequencyCounter,rundate,

            insertTimestamp,RUNID, ACTV_IND,TOTALRUNTIME,SQLRUNTIME,

            METRICID,METRICNAME,RESULT1,PRIORRESULT1,DELTA1,PERCENTCHANGE1,TESTSTATUS1 ,RESULT2,PRIORRESULT2,DELTA2,PERCENTCHANGE2,TESTSTATUS2 ,   

            RESULT3,PRIORRESULT3,DELTA3,PERCENTCHANGE3,TESTSTATUS3,RESULT4,PRIORRESULT4,DELTA4,PERCENTCHANGE4,TESTSTATUS4,

            RESULT5,PRIORRESULT5,DELTA5,PERCENTCHANGE5,TESTSTATUS5,

            RESULT6,PRIORRESULT6,DELTA6,PERCENTCHANGE6,TESTSTATUS6,

            RESULT7,PRIORRESULT7,DELTA7,PERCENTCHANGE7,TESTSTATUS7,

            RESULT8,PRIORRESULT8,DELTA8,PERCENTCHANGE8,TESTSTATUS8,

            RESULT9,PRIORRESULT9,DELTA9,PERCENTCHANGE9,TESTSTATUS9,

            RESULT10,PRIORRESULT10,DELTA10,PERCENTCHANGE10,TESTSTATUS10

            FROM

            (

            select 

    JOBNAME,EXECUTIONSTEP,ENVIRONMENT_ID,TESTCASEDESCRIPTION,NVL(FrequencyCounter,0) as FrequencyCounter ,rundate,insertTimestamp,RUNID,ACTV_IND,TOTALRUNTIME,SQLRUNTIME,METRICID,METRICNAME,RESULT1,

    PRIORRESULT1,DELTA1,PERCENTCHANGE1,TESTSTATUS1,RESULT2,PRIORRESULT2,DELTA2,PERCENTCHANGE2,TESTSTATUS2,   

    RESULT3,PRIORRESULT3,DELTA3,PERCENTCHANGE3,TESTSTATUS3,RESULT4,PRIORRESULT4,DELTA4,PERCENTCHANGE4,TESTSTATUS4,

            RESULT5,PRIORRESULT5,DELTA5,PERCENTCHANGE5,TESTSTATUS5,

            RESULT6,PRIORRESULT6,DELTA6,PERCENTCHANGE6,TESTSTATUS6,

            RESULT7,PRIORRESULT7,DELTA7,PERCENTCHANGE7,TESTSTATUS7,

            RESULT8,PRIORRESULT8,DELTA8,PERCENTCHANGE8,TESTSTATUS8,

            RESULT9,PRIORRESULT9,DELTA9,PERCENTCHANGE9,TESTSTATUS9,

            RESULT10,PRIORRESULT10,DELTA10,PERCENTCHANGE10,TESTSTATUS10 from tmp_fltn pivot (min(rslt) for key in (''METRICID'',''METRICNAME'',''RESULT1'',''PRIORRESULT1'',''DELTA1'',''PERCENTCHANGE1'',''TESTSTATUS1'' ,''RESULT2'',''PRIORRESULT2'',''DELTA2'',''PERCENTCHANGE2'',''TESTSTATUS2'',''RESULT3'',''PRIORRESULT3'',''DELTA3'',''PERCENTCHANGE3'',''TESTSTATUS3'',''RESULT4'',''PRIORRESULT4'',''DELTA4'',''PERCENTCHANGE4'',''TESTSTATUS4'',''RESULT5'',  ''PRIORRESULT5'',''DELTA5'',''PERCENTCHANGE5'',''TESTSTATUS5'',''RESULT6'',''PRIORRESULT6'',''DELTA6'',''PERCENTCHANGE6'',''TESTSTATUS6'',''RESULT7'',''PRIORRESULT7'',''DELTA7'',''PERCENTCHANGE7'',''TESTSTATUS7'',''RESULT8'',''PRIORRESULT8'',''DELTA8'',''PERCENTCHANGE8'',''TESTSTATUS8'',''RESULT9'',''PRIORRESULT9'',''DELTA9'',''PERCENTCHANGE9'',''TESTSTATUS9'',''RESULT10'',''PRIORRESULT10'',''DELTA10'',''PERCENTCHANGE10'',''TESTSTATUS10''))

as finaltable(SEQ,JOBNAME,EXECUTIONSTEP,ENVIRONMENT_ID, TESTCASEDESCRIPTION,FrequencyCounter,rundate,insertTimestamp,RUNID,ACTV_IND,TOTALRUNTIME,SQLRUNTIME,

METRICID,METRICNAME,RESULT1,PRIORRESULT1,DELTA1,PERCENTCHANGE1,TESTSTATUS1 ,RESULT2,PRIORRESULT2,DELTA2,PERCENTCHANGE2,TESTSTATUS2 ,   

RESULT3,PRIORRESULT3,DELTA3,PERCENTCHANGE3,TESTSTATUS3,RESULT4,PRIORRESULT4,DELTA4,PERCENTCHANGE4,TESTSTATUS4,RESULT5,PRIORRESULT5,DELTA5,PERCENTCHANGE5,TESTSTATUS5,RESULT6,PRIORRESULT6,DELTA6,PERCENTCHANGE6,TESTSTATUS6,RESULT7,PRIORRESULT7,DELTA7,PERCENTCHANGE7,TESTSTATUS7,RESULT8,PRIORRESULT8,DELTA8,PERCENTCHANGE8,TESTSTATUS8,RESULT9,PRIORRESULT9,DELTA9,PERCENTCHANGE9,TESTSTATUS9,RESULT10,PRIORRESULT10,DELTA10,PERCENTCHANGE10,TESTSTATUS10))PVT; 

 

--Update previous runs in test results to actv_ind = N''

--Update Active Ind

   

                                                          

                                                          Update DSE_TESTRESULTS D1

                Set D1.ACTV_IND = ''N''

                                                          WHERE D1.DSID =:DSID and ACTV_IND = ''Y''

                                                          and D1.ENVIRONMENT_ID = :v_ENVIRONMENT_ID

                                                          ; --D1.Runid = :v_MAX_RUNID;   



--- Insert any metrics that might be lost from last run to current run

                                                                                      

                             IF (:v_TestType =''MULTI DATAPOINT'') then

                             INSERT INTO TEMP_DSE_DataProfile_MetricList( DSID,Jobname,ExecutionStep,ENVIRONMENT_ID, MetricName,Result1,Result2,Result3,Result4,Result5,Result6,Result7,Result8,Result9,Result10)

                             Select distinct R.DSID, R.Jobname,R.ExecutionStep,r.ENVIRONMENT_ID, COALESCE(R.MetricName,'''') as MetricName,NULL as Result1,NULL as Result2,NULL as  Result3,NULL as Result4,NULL as Result5,NULL as Result6,NULL as Result7,NULL as Result8,NULL as Result9,NULL as Result10 

                             from TEMP_DSE_TESTRESULTS_STG R 

                             left outer join TEMP_DSE_DataProfile_MetricList M on R.DSiD =M.DSID and COALESCE(R.MetricName,'''')=COALESCE(M.MetricName,'''') and R.ENVIRONMENT_ID = M.ENVIRONMENT_ID

                             where R.DSID= :DSID and R.runid =(select max(runid) from TEMP_DSE_TESTRESULTS_STG where runid < :v_RunId 

                                                                and DSID = :DSID and ENVIRONMENT_ID = :v_ENVIRONMENT_ID)

                             and R.metricId<>0 and R.ACTV_IND=''Y''

                             and M.DSId IS NULL and r.ENVIRONMENT_ID = :v_ENVIRONMENT_ID;                                                   

                             end if;       

        

/*BEGIN UPDATE RESULTS INTO TABLES FOR COUNT AND DATE TYPES  */



update TEMP_DSE_DataProfile_MetricList DPM1 

SET DPM1.rowNr = reslt.mtrcId

FROM

    (Select DSID,ENVIRONMENT_ID,MetricName,row_number() over (order by COALESCE(MetricName,'''') asc) as mtrcId

     from TEMP_DSE_DataProfile_MetricList

    )reslt

where DPM1.DSID=reslt.DSID and coalesce(DPM1.MetricName,'''')=coalesce(reslt.MetricName,'''') and DPM1.ENVIRONMENT_ID = reslt.ENVIRONMENT_ID;

 



--'' Update Testcase description, metricId, MetricName, Runid''    



                    update TEMP_DSE_DataProfile_MetricList DPM1 

                    set DPM1.TEstCaseDescription=reslt.TEstCaseDescription,

                        DPM1.MetricId=reslt.MetricId,

                        DPM1.metricName=reslt.metricName,

                        DPM1.RUNID=reslt.RUNID

                                                          FROM(select DPM.DSID,

                                                                  :v_TestCaseDescription as TEstCaseDescription ,

                                                                  case when :v_testtype IN (''SINGLE DATAPOINT'',''MINUS QUERY'',''GETPROFILEKEY'') then 0 

                                                                                                      when :v_testtype = ''MULTI DATAPOINT'' and DPR.MetricId IS NULL and DPR1.DSID IS NULL then DPM.RowNr

                                                                                                     when :v_testtype = ''MULTI DATAPOINT'' and DPR.MetricId IS NULL and DPR1.DSID IS NOT NULL then DPR1.metricId+DPM.RowNr

                                                                                                     when :v_testtype = ''MULTI DATAPOINT'' and DPR.MetricId IS NOT NULL then DPR.MetricId END MetricId,

                                                                                      case when :v_testtype IN (''SINGLE DATAPOINT'',''MINUS QUERY'',''GETPROFILEKEY'') then  :v_TestCaseDescription ELSE COALESCE(DPM.metricName,'''') END  metricName, 

                                                                                      :v_RunId RunId

                                                          FROM TEMP_DSE_DataProfile_MetricList DPM 

                                                                        LEFT JOIN (Select distinct DSID,ENVIRONMENT_ID, MetricId, MetricName from TEMP_DSE_TESTRESULTS_STG where ENVIRONMENT_ID = :v_ENVIRONMENT_ID) DPR 

                                                                        on (DPM.DSID = DPR.DSID and COALESCE(DPM.MetricName,'''') = COALESCE(DPR.MetricName,'''')) and DPM.ENVIRONMENT_ID = DPR.ENVIRONMENT_ID

                                                                        LEFT JOIN (Select distinct DSID,ENVIRONMENT_ID, max(MetricId) as metricId from TEMP_DSE_TESTRESULTS_STG where DSID =:DSID and ENVIRONMENT_ID = :v_ENVIRONMENT_ID group by DSID,ENVIRONMENT_ID) DPR1 

                                                                        on (DPM.DSID = DPR1.DSID and DPM.ENVIRONMENT_ID = DPR1.ENVIRONMENT_ID)

                    ) reslt

                                                          where DPM1.DSID=reslt.DSID and (case when :v_TestType IN (''SINGLE DATAPOINT'',''MINUS QUERY'',''GETPROFILEKEY'') then :v_TestCaseDescription ELSE COALESCE(DPM1.metricName,'''') END=coalesce(reslt.MetricName,''''));

                

               

 --''Update  Prior Results''

                                                                        Update TEMP_DSE_DataProfile_MetricList DPM1

                                                                        Set 

                                                                                      DPM1.priorResult1     = reslt.result1,

                                                                                      DPM1.priorResult2     = reslt.result2,

                                                                                      DPM1.priorResult3     = reslt.result3,

                        DPM1.priorResult4     = reslt.result4,

                        DPM1.priorResult5     = reslt.result5,

                        DPM1.priorResult6     = reslt.result6,

                        DPM1.priorResult7     = reslt.result7,

                        DPM1.priorResult8     = reslt.result8,

                        DPM1.priorResult9     = reslt.result9,

                        DPM1.priorResult10    = reslt.result10,

                                                                            DPM1.FrequencyCounter = reslt.FrequencyCounter

                    FROM

                    (

                    SELECT DPM.DSID,DPM.METRICID,lst.result1,lst.result2,lst.result3,lst.result4,lst.result5,lst.result6,lst.result7,lst.result8,

                        lst.result9,lst.result10,lst.FrequencyCounter,DPM.ENVIRONMENT_ID

                                                                        FROM

                                                                        TEMP_DSE_DataProfile_MetricList DPM 

                                                                        LEFT OUTER JOIN (Select * from (

                                                                                      Select DSID,ENVIRONMENT_ID,metricId,metricName,result1,result2,result3,result4,result5,result6,result7,result8,

                        result9,result10,NVL(FrequencyCounter,0) as FrequencyCounter

                                                                            from  TEMP_DSE_TESTRESULTS_STG

                                                                                      where DSID= :DSID and Runid = :v_MAX_RUNID and ENVIRONMENT_ID = :v_ENVIRONMENT_ID

                                                                        )X)lst on DPM.DSID = lst.DSID

                                                                        and DPM.metricID = lst.metricId 

                                                                        and DPM.ENVIRONMENT_ID = lst.ENVIRONMENT_ID

                    )reslt

                                                                        where DPM1.DSID=reslt.DSID and DPM1.METRICID=reslt.METRICID and DPM1.ENVIRONMENT_ID =reslt.ENVIRONMENT_ID;                  

                                    



--- ADD LOGIC FOR FREQ COUNTER



IF (:v_FrequencyCheck > 0)

THEN

	Update TEMP_DSE_DataProfile_MetricList

	Set FrequencyCounter= case when (coalesce(Result1,''0'')=coalesce(PriorResult1,''0'')

	and coalesce(Result2,''0'')=coalesce(PriorResult2,''0'') 

	and coalesce(Result3,''0'')=coalesce(PriorResult3,''0'')

	and coalesce(Result4,''0'')=coalesce(PriorResult4,''0'')

	and coalesce(Result5,''0'')=coalesce(PriorResult5,''0'')

	and coalesce(Result6,''0'')=coalesce(PriorResult6,''0'')

	and coalesce(Result7,''0'')=coalesce(PriorResult7,''0'')

	and coalesce(Result8,''0'')=coalesce(PriorResult8,''0'')

	and coalesce(Result9,''0'')=coalesce(PriorResult9,''0'')

	and coalesce(Result10,''0'')=coalesce(PriorResult10,''0''))

	then NVL(FrequencyCounter,0)::INT+1 else 0 END 

	where DSID= :DSID and Runid = :v_MAX_RUNID and ENVIRONMENT_ID = :v_ENVIRONMENT_ID ;

END IF;	              

--START LOGIC for TEST STATUS--



 BEGIN

  let counter INTEGER DEFAULT 1;

 

 FOR i IN 1 TO v_RESULTSETCOLS DO



--'' Update Delta, Frequency Counter, PercentChange''

--- DELTA update

Execute Immediate ''Update TEMP_DSE_DataProfile_MetricList

Set Delta''||counter||'' =  

-- result  is numeric or is null

            case when (hash(lower(COALESCE(result''||counter||'',''''0'''')))=hash(upper(COALESCE(result''||counter||'',''''0''''))) OR result''||counter||'' IS NULL ) 

           	  then 

                 --- Prior Result is numeric and not NUll

                 case when (hash(lower(COALESCE(Priorresult''||counter||'',''''0'''')))=hash(Upper(COALESCE(Priorresult''||counter||'',''''0''''))) and Priorresult''||counter||'' IS NOT NULL) 

                 	   then  (result''||counter||''::int - NVL(Priorresult''||counter||'',0)::int)::varchar

                     --Prior Result is not numeric or is null

                     when (hash(lower(COALESCE(Priorresult''||counter||'',''''0'''')))<>hash(Upper(COALESCE(Priorresult''||counter||'',''''0''''))) OR Priorresult''||counter||'' IS NULL) 

                 	   then  

                      		case when result''||counter||'' IS NULL 

                           	then ''''NA'''' ELSE result''||counter||'' 

                           END

           	  	 END

                    -- when result is non-numeric then na

                 when (hash(lower(COALESCE(result''||counter||'',''''0'''')))<>hash(Upper(COALESCE(result''||counter||'',''''0''''))))

                 		then ''''NA''''

                 ELSE ''''NA''''

               END

        Where DSID =''||DSID||'' and ENVIRONMENT_ID = ''||v_ENVIRONMENT_ID||'''';    

---Percent Change update



Execute Immediate ''Update TEMP_DSE_DataProfile_MetricList

Set PercentChange''||counter||'' =  

-- result  is numeric or is null

---case1

            case when (hash(lower(COALESCE(result''||counter||'',''''0'''')))=hash(upper(COALESCE(result''||counter||'',''''0''''))) OR result''||counter||'' IS NULL)  

           	  then 

              --case 2

              		case when Priorresult''||counter||'' IS NOT NULL then

                 		--- Prior Result is numeric and not NUll and >0

                     		---case 3

                            case when (hash(lower(COALESCE(Priorresult''||counter||'',''''0'''')))=hash(Upper(COALESCE(Priorresult''||counter||'',''''0'''')))) 

                     	   			then case when COALESCE(Priorresult''||counter||'',''''0'''')>''''0''''

                                    		then ((NVL(result''||counter||'',0)::int - Priorresult''||counter||''::int)/Priorresult''||counter||''::decimal(19,4))::varchar

                                            ELSE result''||counter||'' END

                                     

                        	 --Prior Result is not numeric or is null

                         			when (hash(lower(COALESCE(Priorresult''||counter||'',''''0'''')))<>hash(Upper(COALESCE(Priorresult''||counter||'',''''0'''')))) 

                     	   			then 

                                    ---case4

                                      	case when result''||counter||'' IS NULL 

                                        then ''''NA'''' ELSE ''''1''''

                                        END -- end case 4

                            END  --- end case 3

                        when NVL(Priorresult''||counter||'',0)=0

                           then 

                           -- case 5

                           		case when ''||v_runid||''= 1 then ''''0''''

                           	         when ''||v_runid||''<> 1 and result''||counter||'' IS NOT NULL then ''''1'''' 

                                     ELSE ''''NA'''' 

                                     END -- end case 5

           	  	     END -- case 2 end

                    -- when result is non-numeric then na

                 when (hash(lower(COALESCE(result''||counter||'',''''0'''')))<>hash(Upper(COALESCE(result''||counter||'',''''0''''))))

                 		then ''''NA''''

                 ELSE ''''NA''''

               END -- case 1 end

        Where DSID =''||DSID||'' and ENVIRONMENT_ID = ''||v_ENVIRONMENT_ID||''''; 

 

 Select distinct try_to_number(split_part(ThresholdId, '','', :counter)) into :v_thresholdid 

 from DATASENTINEL_COMPACT.DSE_TESTPLAN where DSID =:DSID;



IF(:v_thresholdid<>'''') then 

select distinct thresholdtype, NVL(floorvalue,-1), NVL(ceilingvalue,-1), NVL(minwarningthreshold,''NA''), NVL(maxwarningthreshold,''NA''), NVL(minfailthreshold,''NA''), NVL(maxfailthreshold,''NA'')

into v_ThresholdType, v_floorvalue, v_ceilingvalue, v_minwarningthreshold, v_maxwarningthreshold, v_minfailthreshold, v_maxfailthreshold

from datasentinel_compact.dse_threshold where thresholdId =:v_thresholdid;



  Execute Immediate ''Update TEMP_DSE_DataProfile_MetricList DPM1

						Set  DPM1.TESTSTATUS''||counter||'' = reslt.TESTSTATUS''||counter||''     

						FROM      (SELECT DPM.DSID,DPM.METRICID,DPM.ENVIRONMENT_ID,

---case 1 check if the result is numeric or an error value

case when (result''||counter||'' IS NULL AND PriorResult''||counter||'' IS NOT NULL) then ''''PASS''''
     else case when (result''||counter||'' IS NOT NULL AND hash(lower(result''||counter||''))=hash(Upper(result''||counter||'')))



     then

----case 2 check on thresholdtype  

		

                  CASE 

                  	---BEGIN PERCENT BLOCK

                    	WHEN ''''''||v_ThresholdType||'''''' =''''PERCENT'''' 

                  THEN 

                 ----Case 3: Check if Percentchange is numeric or exponential.. if it is exponential FAIL            

                               CASE WHEN hash(lower(DPM.PercentChange''||counter||''))=hash(upper(DPM.PercentChange''||counter||''))

                               THEN

                 ---- case 4: Check if the result is btween floor and ceiling values                   

                                                    case when ((''||v_floorvalue||'' >-1 AND ''||v_ceilingvalue||'' =-1 

                                                    		  		AND Result''||counter||''::INTEGER >= ''||v_floorvalue||''::INTEGER)  -- floor value is not null and ceiling value is null and result>floor value

                                                              OR  (''||v_ceilingvalue||'' >-1 AND ''||v_floorvalue||'' =-1

                                                                    AND Result''||counter||''::INTEGER <= ''||v_ceilingvalue||''::INTEGER) -- ceiling value is not null and floor value is null and result<= ceiling

                                                              OR  (''||v_floorvalue||'' =-1 AND ''||v_ceilingvalue||'' =-1) --- both ceiling and floor values are null

                                                              OR ((''||v_floorvalue||'' >-1 AND ''||v_ceilingvalue||'' >-1) 

                                                                    AND (COALESCE(Result''||counter||'',0)::INTEGER between ''||v_floorvalue||''::INTEGER and ''||v_ceilingvalue||''::INTEGER))) 

                                                                    -- both ceiling and floor are not null and result is between floor and ceiling

                                                          THEN 

                                                ----CAse 5 process for each combination of thresholds

                                                

                                                     --- when all thresholds are populated

                                                             

                                                          CASE WHEN (''''''||v_minwarningthreshold||''''''<>''''NA'''' AND ''''''||v_maxwarningthreshold||'''''' <>''''NA'''' 

                                                        			AND ''''''||v_minfailthreshold||''''''<>''''NA'''' AND ''''''||v_maxfailthreshold||'''''' <>''''NA'''')

                                                                    then 

                                                                        ---case 6: check percent change value between warning and fail thresholds

                                                                        ----        percent change between warning thresholds then pass

                                                                         CASE WHEN PercentChange''||counter||''::decimal(19,4) >= ''''''||v_minwarningthreshold||''''''::decimal(19,4)

                                   											   and PercentChange''||counter||''::decimal(19,4) <= ''''''||v_maxwarningthreshold||''''''::decimal(19,4) then ''''PASS''''

                                                                         	  

                                                                        ------ Percent Change is between min and max warning and fail thresholds          

                                                                              WHEN ((PercentChange''||counter||''::decimal(19,4) < ''''''||v_minwarningthreshold||'''''' 

                                                                                    and PercentChange''||counter||''::decimal(19,4) >= ''''''||v_minfailthreshold||'''''')

                                                                                  OR (PercentChange''||counter||''::decimal(19,4) > ''''''||v_maxwarningthreshold||'''''' 

                                                                                     and PercentChange''||counter||''::decimal(19,4) <= ''''''||v_maxfailthreshold||''''''))  then ''''WARNING''''        

                                                                          -- Percent Chnge is greater than max fail threshold or less than min fail threhsold    

                                                                              WHEN (PercentChange''||counter||''::decimal(19,4) < ''''''||v_minfailthreshold||''''''

                                                                                  OR PercentChange''||counter||''::decimal(19,4) > ''''''||v_maxfailthreshold||'''''')  then ''''FAIL'''' 

                                                                         END -- end case 6     

                                                        -- when only warning thresholds are populated

                                                                   WHEN ((''''''||v_minwarningthreshold||''''''<>''''NA'''' AND ''''''||v_maxwarningthreshold||'''''' <>''''NA'''')

                                                                        and (''''''||v_minfailthreshold||''''''=''''NA'''' OR ''''''||v_maxfailthreshold||'''''' =''''NA''''))

                                                                   then 

                                 -----Case 7: Percent change less than min and greater than max warning thresholds          

                                                                          	 case WHEN PercentChange''||counter||''::decimal(19,4) >= ''''''||v_minwarningthreshold||''''''::decimal(19,4)

                                   											       and PercentChange''||counter||''::decimal(19,4) <= ''''''||v_maxwarningthreshold||''''''::decimal(19,4) then ''''PASS''''

                                                                                   when ((PercentChange''||counter||''::decimal(19,4) < ''''''||v_minwarningthreshold||'''''') 

                                                                          	 			OR (PercentChange''||counter||''::decimal(19,4) >''''''||v_maxwarningthreshold||'''''')) then ''''WARNING''''

                                                                          	 END --- end case 7

                                                           -- min and max warning thresholds are null and fail thresholds are populated

                                                          WHEN ((''''''||v_minwarningthreshold||''''''=''''NA'''' OR ''''''||v_maxwarningthreshold||'''''' =''''NA'''')

                                                                        and (''''''||v_minfailthreshold||''''''<>''''NA'''' AND ''''''||v_maxfailthreshold||'''''' <>''''NA''''))

                                                                   then 

                                    --- case 8: Percent change less than min and greater than max fail thresholds                                       

                                                                      case WHEN PercentChange''||counter||''::decimal(19,4) >= ''''''||v_minfailthreshold||''''''::decimal(19,4)

                                   											       and PercentChange''||counter||''::decimal(19,4) <= ''''''||v_maxfailthreshold||''''''::decimal(19,4) then ''''PASS''''

                                                                         when (PercentChange''||counter||''::decimal(19,4) < ''''''||v_minfailthreshold||''''''

                                                                         	          OR PercentChange''||counter||''::decimal(19,4) > ''''''||v_maxfailthreshold||'''''') then ''''FAIL''''

                                                                         	 END  --end case 8

                                                                 END -- End case 5

                                 when (Result''||counter||''::integer < ''||v_floorvalue||''::INTEGER 

                                                          OR Result''||counter||''::integer > ''||v_ceilingvalue||'') then ''''PASS''''

                                 END --- End Case 4

                                ELSE ''''FAIL''''

                         	  END   --- END Case 3 

                        

                        --- END PERCENT TYPE 



                                                                                             

                       ---- BEGIN NUMBER TYPE

                       WHEN ''''''||v_ThresholdType||'''''' =''''NUMBER''''  

                       THEN                                                                                                                                                                                                                  ---- case 4: Check if the result is btween floor and ceiling values                   

                                                    case when ((''||v_floorvalue||'' >-1 AND ''||v_ceilingvalue||'' =-1 

                                                    		  		AND Result''||counter||''::INTEGER >= ''||v_floorvalue||''::INTEGER)  -- floor value is not null and ceiling value is null and result>floor value

                                                              OR  (''||v_ceilingvalue||'' >-1 AND ''||v_floorvalue||'' =-1

                                                                    AND Result''||counter||''::INTEGER <= ''||v_ceilingvalue||''::INTEGER) -- ceiling value is not null and floor value is null and result<= ceiling

                                                              OR  (''||v_floorvalue||'' =-1 AND ''||v_ceilingvalue||'' =-1) --- both ceiling and floor values are null

                                                              OR ((''||v_floorvalue||'' >-1 AND ''||v_ceilingvalue||'' >-1) 

                                                                    AND (COALESCE(Result''||counter||'',0)::INTEGER between ''||v_floorvalue||''::INTEGER and ''||v_ceilingvalue||''::INTEGER))) 

                                                                    -- both ceiling and floor are not null and result is between floor and ceiling

                                                          THEN 

                                                ----CAse 5 process for each combination of thresholds

                                                

                                                     --- when all thresholds are populated

                                                             

                                                          CASE WHEN (''''''||v_minwarningthreshold||''''''<>''''NA'''' AND ''''''||v_maxwarningthreshold||'''''' <>''''NA'''' 

                                                        			AND ''''''||v_minfailthreshold||''''''<>''''NA'''' AND ''''''||v_maxfailthreshold||'''''' <>''''NA'''')

                                                                    then 

                                                                        ---case 6: check result value between warning and fail thresholds

                                                                        ----        result between warning thresholds then pass

                                                                         CASE WHEN Result''||counter||''::decimal(19,4) >= ''''''||v_minwarningthreshold||''''''::decimal(19,4)

                                   											   and Result''||counter||''::decimal(19,4) <= ''''''||v_maxwarningthreshold||''''''::decimal(19,4) then ''''PASS''''

                                                                         	  

                                                                        ------ Result is between min and max warning and fail thresholds          

                                                                              WHEN ((Result''||counter||''::decimal(19,4) < ''''''||v_minwarningthreshold||'''''' 

                                                                                    and Result''||counter||''::decimal(19,4) >= ''''''||v_minfailthreshold||'''''')

                                                                                  OR (Result''||counter||''::decimal(19,4) > ''''''||v_maxwarningthreshold||''''''

                                                                                     and Result''||counter||''::decimal(19,4) <= ''''''||v_maxfailthreshold||''''''))  then ''''WARNING''''        

                                                                          -- Result is greater than max fail threshold or less than min fail threhsold    

                                                                              WHEN (Result''||counter||''::decimal(19,4) < ''''''||v_minfailthreshold||''''''

                                                                                  OR Result''||counter||''::decimal(19,4) > ''''''||v_maxfailthreshold||'''''')  then ''''FAIL'''' 

                                                                         END -- end case 6     

                                                        -- when only warning thresholds are populated

                                                                   WHEN ((''''''||v_minwarningthreshold||''''''<>''''NA'''' AND ''''''||v_maxwarningthreshold||'''''' <>''''NA'''')

                                                                        and (''''''||v_minfailthreshold||''''''=''''NA'''' OR ''''''||v_maxfailthreshold||'''''' =''''NA''''))

                                                                   then 

                                 -----Case 7: Result less than min and greater than max warning thresholds          

                                                                          	 case WHEN Result''||counter||''::decimal(19,4) >= ''''''||v_minwarningthreshold||''''''::decimal(19,4)

                                   											       and Result''||counter||''::decimal(19,4) <= ''''''||v_maxwarningthreshold||''''''::decimal(19,4) then ''''PASS''''

                                                                                   when ((Result''||counter||''::decimal(19,4) < ''''''||v_minwarningthreshold||'''''') 

                                                                          	 			OR (Result''||counter||''::decimal(19,4) >''''''||v_maxwarningthreshold||'''''')) then ''''WARNING''''

                                                                          	 END --- end case 7

                                                           -- min and max warning thresholds are null and fail thresholds are populated

                                                          WHEN ((''''''||v_minwarningthreshold||''''''=''''NA'''' OR ''''''||v_maxwarningthreshold||'''''' =''''NA'''')

                                                                        and (''''''||v_minfailthreshold||''''''<>''''NA'''' AND ''''''||v_maxfailthreshold||'''''' <>''''NA''''))

                                                                   then 

                                    --- case 8: Result less than min and greater than max fail thresholds                                       

                                                                      case WHEN Result''||counter||''::decimal(19,4) >= ''''''||v_minfailthreshold||''''''::decimal(19,4)

                                   											       and Result''||counter||''::decimal(19,4) <= ''''''||v_maxfailthreshold||''''''::decimal(19,4) then ''''PASS''''

                                                                         when (Result''||counter||''::decimal(19,4) < ''''''||v_minfailthreshold||''''''

                                                                         	          OR Result''||counter||''::decimal(19,4) > ''''''||v_maxfailthreshold||'''''') then ''''FAIL''''

                                                                         	 END  --end case 8

                                                                 END -- End case 5

                                 when (Result''||counter||''::integer < ''||v_floorvalue||''::INTEGER 

                                                          OR Result''||counter||''::integer > ''||v_ceilingvalue||'') then ''''PASS''''

                                 END --- End Case 4

                  ELSE ''''FAIL'''' END -- END CASE 2

          ELSE ''''FAIL'''' END --- END CASE 1         

          as TESTSTATUS''||counter||''

                                                                                      FROM TEMP_DSE_DataProfile_MetricList DPM 

                                                                                      WHERE DPM.DSID = ''||DSID||'' and DPM.ENVIRONMENT_ID = ''||v_ENVIRONMENT_ID||''

           )reslt

            where DPM1.DSID=reslt.DSID and DPM1.METRICID=reslt.METRICID and DPM1.ENVIRONMENT_ID= reslt.ENVIRONMENT_ID '';

END IF;

        --- Frequency Check                                                                           

        Execute Immediate ''Update TEMP_DSE_DataProfile_MetricList DPM1

                           Set 

                           DPM1.TestStatus''||counter||''     = reslt.TestStatus

                		   FROM

                		   (

                		       SELECT DPM.DSID,DPM.METRICID,DPM.ENVIRONMENT_ID, ''''WARNING'''' as TestStatus

                		       FROM TEMP_DSE_DataProfile_MetricList DPM 

                		       WHERE DPM.DSID = ''||DSID||'' and DPM.ENVIRONMENT_ID = ''||v_ENVIRONMENT_ID||''

                		       AND DPM.FrequencyCounter>''||v_frequencycheck||'' and DPM.FrequencyCounter>0

                		   )reslt

                          where DPM1.DSID=reslt.DSID and DPM1.METRICID=reslt.METRICID and DPM1.ENVIRONMENT_ID = reslt.ENVIRONMENT_ID'';



      Execute Immediate ''Update TEMP_DSE_DataProfile_MetricList DPM1

                              Set 

                              DPM1.TestStatus''||counter||'' = reslt.TestStatus

         FROM

                     (

                     SELECT DPM.DSID,CASE WHEN (regexp_instr(RESULT''||counter||'',''''[A-Z]'''',1,1,0,''''i'''') = 0 and coalesce(METRICNAME,'''''''') <> ''''ERROR'''') THEN ''''PASS'''' ELSE ''''FAIL'''' END as TestStatus                         

                     from TEMP_DSE_DataProfile_MetricList DPM

                     left outer join DSE_TestResults R on DPM.DSID =R.DSID and DPM.ENVIRONMENT_ID  =R.ENVIRONMENT_ID

                     where R.DSID IS NULL

                     )reslt

                     where DPM1.DSID = reslt.DSID'';              

                        

       

    counter := counter + 1;













    

  END FOR;





  EXCEPTION



                                           when statement_error then

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

                                          

           return 1;

           

                                          when expression_error then 

                                          v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                          execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

                                           

            return 1;

            

                                           when other then 

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

           

           return 1;

  

END;





--                                                       

------------------------------------------------------- END LOOP

 

--Below is for scenario when a new Test case is added

            



        

             

--Print ''Update Test Status for Minus Query''

                    Update TEMP_DSE_DataProfile_MetricList DPM1

                    Set 

                    DPM1.TESTSTATUS1 = reslt.TESTSTATUS1

                    FROM

                    (

                      SELECT DPM.DSID, DPM.METRICID,

                      CASE WHEN COALESCE(Result1,''0'')=''0'' and Result1 IS NOT NULL then ''PASS''

                                     WHEN Result1 IS NULL then NULL 

                                      WHEN COALESCE(Result1,''0'')<>''0'' and Result1 IS NOT NULL THEN ''FAIL'' END as TESTSTATUS1

                      FROM TEMP_DSE_DataProfile_MetricList DPM 

                      

                      WHERE DPM.DSID = :DSID AND :v_TestType=''MINUS QUERY''

                    )reslt where DPM1.DSID = reslt.DSID and DPM1.METRICID=reslt.METRICID;

                    

                        

--INSERT A ROW WITH METRICID =0 and TESTCASE DESCRIPTION FOR MDP--*****************

        

                             IF (:v_TestType =''MULTI DATAPOINT'') then

                                                          INSERT INTO TEMP_DSE_DataProfile_MetricList            

                                                          Select distinct DPM.DSID, 

                DPM.JobName,

                DPM.ExecutionStep, 

                                                          DPM.ENVIRONMENT_ID,

                DPM.TestCaseDescription,

                0::integer as MetricId,

                DPM.TestCaseDescription as MetricName,

                                                          NULL::varchar as Result1,

                NULL::varchar as PriorResult1,

                NULL::varchar as Delta1,

                NULL::varchar as PercentChange1,

                NULL::varchar as TestStatus1,

                                                          NULL::varchar as Result2,

                NULL::varchar as PriorResult2,

                NULL::varchar as Delta2,

                NULL::varchar as PercentChange2,

                NULL::varchar as TestStatus2,      

                NULL::varchar as Result3,

                NULL::varchar as PriorResult3,

                NULL::varchar as Delta3,

                NULL::varchar as PercentChange3,

                NULL::varchar as TestStatus3,  

                NULL::varchar as Result4,

                NULL::varchar as PriorResult4,

                NULL::varchar as Delta4,

                NULL::varchar as PercentChange4,

                NULL::varchar as TestStatus4,  

                NULL::varchar as Result5,

                NULL::varchar as PriorResult5,

                NULL::varchar as Delta5,

                NULL::varchar as PercentChange5,

                NULL::varchar as TestStatus5,  

                NULL::varchar as Result6,

                NULL::varchar as PriorResult6,

                NULL::varchar as Delta6,

                NULL::varchar as PercentChange6,

                NULL::varchar as TestStatus6,     

                NULL::varchar as Result7,

                NULL::varchar as PriorResult7,

                NULL::varchar as Delta7,

                NULL::varchar as PercentChange7,

                NULL::varchar as TestStatus7,   

                NULL::varchar as Result8,

                NULL::varchar as PriorResult8,

                NULL::varchar as Delta8,

                NULL::varchar as PercentChange8,

                NULL::varchar as TestStatus8,     

                NULL::varchar as Result9,

                NULL::varchar as PriorResult9,

                NULL::varchar as Delta9,

                NULL::varchar as PercentChange9,

                NULL::varchar as TestStatus9,    

                NULL::varchar as Result10,

                NULL::varchar as PriorResult10,

                NULL::varchar as Delta10,

                NULL::varchar as PercentChange10,

                NULL::varchar as TestStatus10,                   

                0::integer    as FrequencyCounter,

                0 as rowNr,

                :v_RunId as RUNID

                FROM TEMP_DSE_DataProfile_MetricList DPM 

                WHERE DPM.DSID = :DSID;              

                             end if;                

        

                                                          

                

                                                          --Print ''Insert into  DSE_Test Results from MetricList''

                                                          INSERT INTO TEMP_DSE_TESTRESULTS_STG

                (

                    DSID,JobName, Executionstep,ENVIRONMENT_ID,TestCaseDescription,MetricId,MetricName,

                                                                        Result1,PriorResult1,Delta1,PercentChange1,TestStatus1,

                                                                        Result2,PriorResult2,Delta2,PercentChange2,TestStatus2,

                                                                        Result3,PriorResult3,Delta3,PercentChange3,TestStatus3,

                    Result4,PriorResult4,Delta4,PercentChange4,TestStatus4,

                    Result5,PriorResult5,Delta5,PercentChange5,TestStatus5,

                    Result6,PriorResult6,Delta6,PercentChange6,TestStatus6,

                    Result7,PriorResult7,Delta7,PercentChange7,TestStatus7,

                    Result8,PriorResult8,Delta8,PercentChange8,TestStatus8,

                    Result9,PriorResult9,Delta9,PercentChange9,TestStatus9,

                    Result10,PriorResult10,Delta10,PercentChange10,TestStatus10,

                    FrequencyCounter,ACTV_IND,RUNID,RUNDATE,INSERTTIMESTAMP,RPT_MO_KEY,TOTALRUNTIME,SQLRUNTIME                   

                )

                                                                        Select DSID,JobName, Executionstep,ENVIRONMENT_ID,TestCaseDescription,MetricId,MetricName,

                                                                        Result1,PriorResult1,Delta1,PercentChange1,TestStatus1,

                                                                        Result2,PriorResult2,Delta2,PercentChange2,TestStatus2,

                                                                        Result3,PriorResult3,Delta3,PercentChange3,TestStatus3,

                    Result4,PriorResult4,Delta4,PercentChange4,TestStatus4,

                    Result5,PriorResult5,Delta5,PercentChange5,TestStatus5,

                    Result6,PriorResult6,Delta6,PercentChange6,TestStatus6,

                    Result7,PriorResult7,Delta7,PercentChange7,TestStatus7,

                    Result8,PriorResult8,Delta8,PercentChange8,TestStatus8,

                    Result9,PriorResult9,Delta9,PercentChange9,TestStatus9,

                    Result10,PriorResult10,Delta10,PercentChange10,TestStatus10,           

                                                          NVL(FrequencyCounter,0) as FrequencyCounter ,''Y'',:v_RunId,:v_rundate,sysdate(),NULL,NULL,NULL

                                                                        from  TEMP_DSE_DataProfile_MetricList;           

                    

--- Update ACTV_IND =N for records where current and prior results are NULL for a metricid fo multi datapoint



Update TEMP_DSE_TESTRESULTS_STG
Set Actv_ind =''N''
where actv_ind =''Y''
and coalesce(result1,result2,result3,result4,result5,result6,result7,result8,result9,result10,''nan'')=''nan''
and coalesce(priorresult1,priorresult2,priorresult3,priorresult4,priorresult5,priorresult6,priorresult7,priorresult8,priorresult9,priorresult10,''nan'')=''nan'' 
and metricId<>0 
and DSID = :DSID and runid = :v_RunId; 


                INSERT INTO DSE_TESTRESULTS

                (DSID,JOBNAME,EXECUTIONSTEP,ENVIRONMENT_ID,TESTCASEDESCRIPTION,METRICRESULTS,FREQUENCYCOUNTER

                 ,ACTV_IND,RUNID,RUNDATE,INSERTTIMESTAMP,RPT_MO_KEY,TOTALRUNTIME,SQLRUNTIME ) 

                SELECT distinct DSID,JobName, Executionstep,ENVIRONMENT_ID,TestCaseDescription,                

                (Select array_agg((object_construct_keep_null(*)::Variant)) 

                    from

                    (

                    Select metricid, metricname,Result1,PriorResult1,Delta1,PercentChange1,TestStatus1,

                                                                        Result2,PriorResult2,Delta2,PercentChange2,TestStatus2,

                                                                        Result3,PriorResult3,Delta3,PercentChange3,TestStatus3,

                    Result4,PriorResult4,Delta4,PercentChange4,TestStatus4,

                    Result5,PriorResult5,Delta5,PercentChange5,TestStatus5,

                    Result6,PriorResult6,Delta6,PercentChange6,TestStatus6,

                    Result7,PriorResult7,Delta7,PercentChange7,TestStatus7,

                    Result8,PriorResult8,Delta8,PercentChange8,TestStatus8,

                    Result9,PriorResult9,Delta9,PercentChange9,TestStatus9,                        

                    Result10,PriorResult10,Delta10,PercentChange10,TestStatus10

                    from TEMP_DSE_TESTRESULTS_STG  WHERE jobname = :v_JobName and ExecutionStep=UPPER(:v_Step)  and Runid =:v_RunId and ENVIRONMENT_ID = :v_ENVIRONMENT_ID and actv_ind =''Y''

                    )fieldsforvariant

                ) as METRICRESULTS        

                ,NVL(FREQUENCYCOUNTER,0) as FrequencyCounter

                ,ACTV_IND,RUNID,RUNDATE,INSERTTIMESTAMP,RPT_MO_KEY,TOTALRUNTIME,SQLRUNTIME

                from TEMP_DSE_TESTRESULTS_STG 

                WHERE jobname = :v_JobName and ExecutionStep=UPPER(:v_Step)  and Runid =:v_RunId and ENVIRONMENT_ID = :v_ENVIRONMENT_ID and actv_ind =''Y'';  

                         

                

                --Update TESTSTATUS for ''SINGLE DATAPOINT''



                           update DSE_TESTRESULTS DT
        set DT.TESTSTATUS = R.TestStatus1
        from (
          select DSID, RUNID, ENVIRONMENT_ID, JOBNAME, EXECUTIONSTEP, TestStatus1
          from TEMP_DSE_DataProfile_MetricList
          where jobname = :v_JobName
            and executionstep = upper(:v_Step)
            and runid = :v_RunId
            and environment_id = :v_ENVIRONMENT_ID
        ) R
        where DT.DSID = R.DSID
          and DT.RUNID = R.RUNID
          and DT.ENVIRONMENT_ID = R.ENVIRONMENT_ID
          and DT.JOBNAME = R.JOBNAME
          and DT.EXECUTIONSTEP = R.EXECUTIONSTEP
          and :v_TestType in (''SINGLE DATAPOINT'',''MINUS QUERY'',''GETPROFILEKEY'');

                   

                

                 --Update TESTSTATUS for ''MULTI DATAPOINT''

                update DSE_TESTRESULTS DT
set DT.TESTSTATUS =
  case
    when MM.MINTESTSTATUS=''FAIL'' or MM.MAXTESTSTATUS=''FAIL'' then ''FAIL''
    when MM.MINTESTSTATUS=''PASS'' and MM.MAXTESTSTATUS=''WARNING'' then ''WARNING''
    when MM.MINTESTSTATUS=''PASS'' and MM.MAXTESTSTATUS=''PASS'' then ''PASS''
    else MM.MINTESTSTATUS
  end
from (
  select DSID, RUNID, ENVIRONMENT_ID, JOBNAME, EXECUTIONSTEP,
         min(TESTSTATUS) as MINTESTSTATUS,
         max(TESTSTATUS) as MAXTESTSTATUS
  from (
    select DSID, RUNID, ENVIRONMENT_ID, JOBNAME, EXECUTIONSTEP,
           case
             when coalesce(TestStatus1,''PASS'')=''PASS''
              and coalesce(TestStatus2,''PASS'')=''PASS''
              and coalesce(TestStatus3,''PASS'')=''PASS''
              and coalesce(TestStatus4,''PASS'')=''PASS''
              and coalesce(TestStatus5,''PASS'')=''PASS''
              and coalesce(TestStatus6,''PASS'')=''PASS''
              and coalesce(TestStatus7,''PASS'')=''PASS''
              and coalesce(TestStatus8,''PASS'')=''PASS''
              and coalesce(TestStatus9,''PASS'')=''PASS''
              and coalesce(TestStatus10,''PASS'')=''PASS'' then ''PASS''
             when ''FAIL'' in (TestStatus1,TestStatus2,TestStatus3,TestStatus4,TestStatus5,
                             TestStatus6,TestStatus7,TestStatus8,TestStatus9,TestStatus10) then ''FAIL''
             else ''WARNING''
           end as TESTSTATUS
    from TEMP_DSE_DataProfile_MetricList
    where metricid<>0
      and jobname = :v_JobName
      and executionstep = upper(:v_Step)
      and runid = :v_RunId
      and environment_id = :v_ENVIRONMENT_ID
  ) t
  group by DSID, RUNID, ENVIRONMENT_ID, JOBNAME, EXECUTIONSTEP
) MM
where DT.DSID = MM.DSID
  and DT.RUNID = MM.RUNID
  and DT.ENVIRONMENT_ID = MM.ENVIRONMENT_ID
  and DT.JOBNAME = MM.JOBNAME
  and DT.EXECUTIONSTEP = MM.EXECUTIONSTEP
  and :v_TestType in (''MULTI DATAPOINT'');                           





  

EXCEPTION

                                           when statement_error then

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

                                           

            return :v_res_failed;

            

                                           when expression_error then 

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

                                           

            return :v_res_failed;

            

                                           when other then 

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

            

            return :v_res_failed;



                                                                        INSERT INTO DSE_TESTRESULTS(DSID,JOBNAME,EXECUTIONSTEP,ENVIRONMENT_ID,TESTCASEDESCRIPTION,METRICRESULTS,TESTSTATUS,FREQUENCYCOUNTER

                 ,ACTV_IND,RUNID,RUNDATE,INSERTTIMESTAMP,RPT_MO_KEY,TOTALRUNTIME,SQLRUNTIME) 

                                                                        Select :DSID,:v_JobName,:v_STEP, :v_ENVIRONMENT_ID,''Test Info could not be retrieved'',(0||'' ''||''Metric Info couldnot be retrieved'')::Variant,''FAIL''

                   ,0,''Y'',:v_RunId,sysdate(),sysdate(),NULL,NULL,NULL;

                    

                    

                                                          Update DSE_TESTRESULTS D1 

                Set D1.ACTV_IND = reslt.ACTV_IND  

                FROM

                (SELECT DSID,''N'' as ACTV_IND

                                                          FROM DSE_TESTRESULTS D

                                                          Where D.Runid < :v_RunId

                                                          and D.DSID = :DSID and D.ENVIRONMENT_ID =:v_ENVIRONMENT_ID

                )reslt 

                where D1.DSID = reslt.DSID and D1.ENVIRONMENT_ID =reslt.ENVIRONMENT_ID;

                

        



END;    

--ELSE 

    

--    BEGIN 

--  let v_TargetTable_insert varchar := ''Insert into ''|| :v_Targettable||'' Select * from ''|| :Tgt_tmp;

--

--  execute immediate :v_TargetTable_insert; 

--

--





--END;  

  

--return ''Success''; 



end if;



---set returncode DSE_TESTRESULTS

                                           IF (upper(:v_targettable)  = ''DSE_TESTRESULTS'' ) then

                                                                        v_returncode := (Select max(returnstatus) from (

                                                                                                                                 select metricId, 

                                                                                                                                 case when TestStatus1=''PASS'' and teststatus2=''PASS'' and teststatus3=''PASS'' and teststatus4=''PASS'' and teststatus5=''PASS''and teststatus6=''PASS'' and teststatus7=''PASS'' and       teststatus8=''PASS'' and teststatus9=''PASS'' and teststatus10=''PASS'' then 0

                                                                                                                                                when teststatus1=''FAIL'' OR teststatus2=''FAIL'' OR teststatus3=''FAIL'' OR teststatus4=''FAIL'' OR teststatus5=''FAIL'' OR teststatus6=''FAIL'' OR teststatus7=''FAIL'' OR teststatus8=''FAIL'' OR teststatus9=''FAIL'' OR teststatus10=''FAIL'' then 1

                                         when (teststatus1 =''WARNING'' and (teststatus2 <>''FAIL'' and teststatus3<>''FAIL'' and teststatus4 <>''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                                                                                                   OR teststatus2 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus3<>''FAIL'' and teststatus4<>''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                                                                                                   OR teststatus3 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus4<>''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus4 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus5 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus4=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus6 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus4=''FAIL'' and teststatus5=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus7 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus4=''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus8 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus4=''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus9=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus9 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus4=''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus10=''FAIL'')

                                OR teststatus10 =''WARNING'' and (teststatus1 <>''FAIL'' and teststatus2<>''FAIL'' and teststatus3<>''FAIL'' and teststatus4=''FAIL'' and teststatus5=''FAIL'' and teststatus6=''FAIL'' and teststatus7=''FAIL'' and teststatus8=''FAIL'' and teststatus9=''FAIL'')

                                               ) then 2

                                                                                                                                 ELSE 0 END as returnstatus from TEMP_DSE_DataProfile_MetricList)x

                                  );

                                           ELSE 

                v_returncode := 0;

            end if;      



              

        select  sysdate() into :v_DSEndTime;        



       

        Update DSE_TESTRESULTS

        Set TotalRuntime=TIMESTAMPDIFF(second, :v_DSStartTime, :v_DSEndTime),

        SQLRunTime=TIMESTAMPDIFF(second, :v_DSStartTime, :v_DSEndTime)

        where DSID =:DSID and RunID=:v_RunId and rundate=:v_Rundate and ENVIRONMENT_ID = :v_ENVIRONMENT_ID;         

        



--Drop Temporary Tables at the end of the Procedure after each run



--drop table TEMP_DSE_DataSentinel_TestCaseList;

--drop table TEMP_DSE_DataProfile_MetricList;

--drop table TEMP_DSE_TESTRESULTS_STG;



return :v_returncode;



EXCEPTION



                                           when statement_error then

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

                                          

           return 1;

           

                                          when expression_error then 

                                          v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                          execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

                                           

            return 1;

            

                                           when other then 

                                           v_res_failed := regexp_replace(''''||sqlerrm||'''', ''[^a-zA-Z0-9]+'','' '',1,0);

                                           execute immediate ''INSERT INTO SP_DATASENTINEL_LOG(RUNID,SP_Name,jobname ,step ,Msg,STATUS_TYPE,RETURN_CD) select ''''No RUNID'''',''''SP_DS_DSE_ENGINE_CALL'''', ''''No JOB'''',''''No Step'''',''''''||v_res_failed||'''''',''''Error'''',''''1'''';'';

           

           return 1;







end';
