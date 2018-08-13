
TRUNCATE TABLE DWH.O_PRODUCT_TP1;

INSERT INTO DWH.O_PRODUCT_TP1 (
      PRODUCT_ID                              
      ,PRODUCT_NAME                            
      ,SUPPLIER_ID                             
      ,PRODUCTTYPE_ID                          
      ,UPDATED_DTM                             
)
      SELECT
       MA.PRODUCT_ID                      AS PRODUCT_ID
      ,MA.PRODUCT_NAME                    AS PRODUCT_NAME
      ,MA.SUPPLIER_ID                     AS SUPPLIER_ID
      ,MA.PRODUCTTYPE_ID                  AS PRODUCTTYPE_ID
      ,'20171001'                         AS UPDATED_DTM
FROM STAGING.PRODUCT as MA  
;


TRUNCATE TABLE DWH.O_PRODUCT;

INSERT INTO DWH.O_PRODUCT (
      PRODUCT_ID                              
      ,PRODUCT_NAME                            
      ,SUPPLIER_ID                             
      ,PRODUCTTYPE_ID                          
      ,UPDATED_DTM                             
) 
      SELECT
      MA.PRODUCT_ID                           
      ,MA.PRODUCT_NAME                         
      ,MA.SUPPLIER_ID                          
      ,MA.PRODUCTTYPE_ID                       
      ,MA.UPDATED_DTM                          
FROM DWH.O_PRODUCT_TP1 as MA ; 


-- Update statistics
;

