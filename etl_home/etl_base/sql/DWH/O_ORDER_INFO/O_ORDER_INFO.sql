---------------------------------------------------------------------
-- {{DWH}}.SQL script for PG, generated by Macro        
-- Date Time       : 7/30/2018 7:11:45 PM
-- Target Table    : O_ORDER_INFO
-- Script File     : O_ORDER_INFO.sql
-- Programmer      : JESSE
-- Function        : From (ORDER_INFO) tables  into {{DWH}}.O_ORDER_INFO
-- Remarks         :                                                 
---------------------------------------------------------------------
-----------------     Revision History      -------------------------
--   Seq. No.    DATE         By         REASON 
---- -------- ----------- ----------- -------------------------------
--                                                                   
---------------------------------------------------------------------




-- exec BEFORE trigger at PROGRAM level

SET SEARCH_PATH TO DWH;;



TRUNCATE TABLE DWH.O_ORDER_INFO_TP1;

INSERT INTO DWH.O_ORDER_INFO_TP1 (
      ORDER_ID                                
      ,CUSTOMER_ID                             
      ,CREATE_DTM                              
)
      SELECT
       MA.ORDER_ID                        AS ORDER_ID
      ,MA.CUSTOMER_ID                     AS CUSTOMER_ID
      ,MA.CREATE_DTM                      AS CREATE_DTM
FROM STAGING.ORDER_INFO as MA  
WHERE MA.CREATE_DTM = %(window_start_date)s
;


TRUNCATE TABLE DWH.O_ORDER_INFO;

INSERT INTO DWH.O_ORDER_INFO (
      ORDER_ID                                
      ,CUSTOMER_ID                             
      ,CREATE_DTM                              
) 
      SELECT
      MA.ORDER_ID                             
      ,MA.CUSTOMER_ID                          
      ,MA.CREATE_DTM                           
FROM DWH.O_ORDER_INFO_TP1 as MA ; 


-- Update statistics
;


