!connect jdbc:hive2://10.140.10.12:13001/psa_shark dummy dummy org.apache.hive.jdbc.HiveDriver
use psa_shark;
DROP TABLE IF EXISTS sales_sessions_tactic_cached;
CREATE TABLE sales_sessions_tactic_cached AS 
SELECT
    SessionSteps.id as StepID,
    SalesSessions.id as SessionID,
    SalesSessions.name as SessionName,
    SalesTactics.id as TacticID,
    SalesTactics.SalesTacticName as TacticName
FROM
    psa.SalesSessionSteps_stg SessionSteps 
INNER JOIN psa.SalesSessions_stg SalesSessions ON SessionSteps.SalesSessionID = SalesSessions.ID
LEFT OUTER JOIN psa.SalesTactics_stg SalesTactics ON SalesTactics.ID = SalesSessions.SalesTacticID;   
