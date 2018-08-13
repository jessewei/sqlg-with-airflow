
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
      ,MA.UPDATED_DTM                     AS UPDATED_DTM
FROM STAGING.CUSTOMER as MA  
WHERE MA.UPDATED_DTM = '20171001'
;

-- Insert Duplicate Data Into Exception Table
TRUNCATE TABLE DWH.O_CUSTOMER_TPX;
INSERT INTO DWH.O_CUSTOMER_TPX (
      CUSTOMER_ID                             
      ,CUST_NAME                               
      ,STREET                                  
      ,CITY                                    
      ,UPDATED_DTM                             
)
SELECT
      CUSTOMER_ID                             
      ,CUST_NAME                               
      ,STREET                                  
      ,CITY                                    
      ,UPDATED_DTM                             
FROM DWH.O_CUSTOMER_TP1
WHERE (CUSTOMER_ID) IN (SELECT CUSTOMER_ID FROM DWH.O_CUSTOMER_TP1 GROUP BY CUSTOMER_ID HAVING COUNT(*) >1);

SELECT  DWH.SP_CHK_UNIQ( 'O_CUSTOMER_TP1', 'O_CUSTOMER_TPX');

-- Delete data to avoid duplicate perform
DELETE FROM DWH.O_CUSTOMER MA
 WHERE EXISTS (SELECT 1 FROM DWH.O_CUSTOMER_TP1 MB
      WHERE MA.CUSTOMER_ID=MB.CUSTOMER_ID
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



SELECT  DWH.SP_CHK_UNIQ( 'O_CUSTOMER', 'O_CUSTOMER_TPX');

-- Update statistics
;

