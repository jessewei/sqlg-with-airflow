
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
WHERE MA.CREATE_DTM = '20171001'
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

