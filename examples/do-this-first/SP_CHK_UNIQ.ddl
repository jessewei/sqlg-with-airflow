CREATE OR REPLACE FUNCTION DWH.SP_CHK_UNIQ(tabname varchar, excname varchar) RETURNS integer AS $$
DECLARE
SqlStmt VARCHAR(512);
v_no_data SMALLINT DEFAULT 0;
v_cnt INTEGER DEFAULT 0;
c_sqlstmt REFCURSOR;
-- DECLARE CONTINUE HANDLER FOR NOT FOUND  SET v_no_data=1;

BEGIN

--  performance issue
    SqlStmt := 'SELECT count(*) FROM ' || excname || ' LIMIT 1';   
    -- PREPARE v_cur_stmt FROM SqlStmt;
    OPEN c_sqlstmt FOR EXECUTE SqlStmt;

    FETCH c_sqlstmt INTO v_cnt;

----    call sqlext.log(v_cnt || '::' || SqlStmt);
    IF (v_cnt > 0)  THEN
        RAISE NOTICE 'Primary key duplicated. TABLE NAME = %' , tabname;
       RETURN 0;
    ELSE
        RAISE NOTICE 'No duplicated records. TABLE NAME = %' , tabname;
       RETURN 1;
    END IF;
END; $$
LANGUAGE PLPGSQL;
