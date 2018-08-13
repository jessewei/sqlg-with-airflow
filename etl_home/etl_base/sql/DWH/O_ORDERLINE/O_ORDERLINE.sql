
TRUNCATE TABLE DWH.O_ORDERLINE_TP1;

INSERT INTO DWH.O_ORDERLINE_TP1 (
      ORDERLINE_ID                            
      ,ORDER_ID                                
      ,PRODUCT_ID                              
      ,QUANTITY                                
      ,PRICE                                   
)
      SELECT
       MA.ORDERLINE_ID                    AS ORDERLINE_ID
      ,MA.ORDER_ID                        AS ORDER_ID
      ,MA.PRODUCT_ID                      AS PRODUCT_ID
      ,MA.QUANTITY                        AS QUANTITY
      ,MA.PRICE                           AS PRICE
FROM STAGING.ORDERLINE as MA  
;


TRUNCATE TABLE DWH.O_ORDERLINE;

INSERT INTO DWH.O_ORDERLINE (
      ORDERLINE_ID                            
      ,ORDER_ID                                
      ,PRODUCT_ID                              
      ,QUANTITY                                
      ,PRICE                                   
) 
      SELECT
      MA.ORDERLINE_ID                         
      ,MA.ORDER_ID                             
      ,MA.PRODUCT_ID                           
      ,MA.QUANTITY                             
      ,MA.PRICE                                
FROM DWH.O_ORDERLINE_TP1 as MA ; 


-- Update statistics
;

