
TRUNCATE TABLE DWH.O_CUSTOMER_TP1;

INSERT INTO DWH.O_CUSTOMER_TP1 (
      CUSTOMER_ID                             
      ,CUST_NAME                               
      ,STREET                                  
      ,CITY                                    
      ,UPDATED_DTM                             
)
      SELECT
       MA.CUSTOMER_ID                     AS CUSTOMER_ID
      ,MA.CUST_NAME                       AS CUST_NAME
      ,MA.STREET                          AS STREET
      ,MA.CITY                            AS CITY
      ,'20171001'                         AS UPDATED_DTM
FROM STAGING.CUSTOMER as MA  
;


-- Delete data to avoid duplicate perform
DELETE FROM DWH.O_CUSTOMER MA
 WHERE EXISTS (SELECT 1 FROM DWH.O_CUSTOMER_TP1 MB
);
INSERT INTO DWH.O_CUSTOMER (
      CUSTOMER_ID                             
      ,CUST_NAME                               
      ,STREET                                  
      ,CITY                                    
      ,UPDATED_DTM                             
) 
      SELECT
      MA.CUSTOMER_ID                          
      ,MA.CUST_NAME                            
      ,MA.STREET                               
      ,MA.CITY                                 
      ,MA.UPDATED_DTM                          
FROM DWH.O_CUSTOMER_TP1 as MA ; 


-- Update statistics
;

