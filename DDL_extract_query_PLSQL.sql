DECLARE
 	M_SQL VARCHAR2(4000);
	M_CNT NUMBER;

	CURSOR cur (P_OWNER VARCHAR2)
	   IS SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_OWNER = P_OWNER ;
		M_DDL VARCHAR2(8000);
		M_OWNER VARCHAR2(50);
BEGIN
	/*if a TTMP_INDEX_DDL table which will save DDL statements doesn't exist, create a TTMP_INDEX_DDL table*/
 	SELECT COUNT(*) INTO M_CNT FROM USER_TABLES WHERE TABLE_NAME = 'TTMP_INDEX_DDL';
 	M_OWNER := 'ECMS';
 	IF M_CNT = 0 THEN
  	M_SQL := ' CREATE TABLE TTMP_INDEX_DDL (CONTENTS_1 VARCHAR2(4000))';
    EXECUTE IMMEDIATE M_SQL;
    END IF;

 	M_SQL := ' TRUNCATE TABLE TTMP_INDEX_DDL ';
    EXECUTE IMMEDIATE M_SQL;


FOR REC IN cur(M_OWNER) LOOP
SELECT DBMS_METADATA.GET_DDL('INDEX', REC.INDEX_NAME, M_OWNER) INTO M_DDL FROM DUAL;
INSERT INTO TTMP_INDEX_DDL (CONTENTS_1) VALUES
  (SUBSTR(M_DDL, 1, INSTR(M_DDL, 'PCT') -1));
 COMMIT;

 END LOOP;

END;