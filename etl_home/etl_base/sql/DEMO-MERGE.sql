
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

