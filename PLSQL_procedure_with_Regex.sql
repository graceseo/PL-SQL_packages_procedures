create or replace
PROCEDURE login_insert(
                                                        P_MSRL NUMBER,
                                                        P_SSO_SRL NUMBER,
                                                        P_LOGIN_IP VARCHAR2,
                                                        P_LOGIN_TP VARCHAR2,
                                                        P_WIDGET_REF VARCHAR2,
                                                        P_SSO  varchar2)
IS
        app_ver tlg_login_log.m_app_ver%type;
        platform_nm tlg_login_log.m_platform_nm%type;
        platform_ver tlg_login_log.m_platform_ver%type;
        comp_nm  tlg_login_log.m_comp_nm%type;
        device_nm tlg_login_log.m_device_nm%type;
        device_no tlg_login_log.m_device_no%type;
        mm_exception tlg_login_log.m_exception%type;

        m_lenght number;
        m_p_sso varchar2(200);
        m_var varchar2(200);
        m_num number;
        m_cnt number:=1;
        m_temp_pattern varchar2(200);
        m_device_nm_start number;
        m_device_nm_end number;
BEGIN

IF P_LOGIN_TP='M' THEN 
--정해진 패턴이 없으면 예외처리한다.
        if (REGEXP_SUBSTR (p_sso,'i[Pp]hone\s|i[Pp]od\s|i[Pp]ad\s|[Aa]ndroid\s|[Ww]indows mobile\s') is null) 
           or  REGEXP_SUBSTR(p_sso,'^Bugs\s')  is null then
                mm_exception:=P_SSO;
                --update로 변경하기!!
                insert into tlg_login_log (msrl,sso_srl,login_ip1,login_tp,login_dt,widget_ref,m_exception) 
                                          values (P_MSRL,P_SSO_SRL,P_LOGIN_IP,P_LOGIN_TP,to_char(sysdate,'yyyymmddhh24miss'),P_WIDGET_REF,mm_exception);
                commit;
                return; 
        end if;
 
 
 --첫글짜가 Bugs와공백이면 Bugs부터  모든문자/숫자 제외한 텍스트만 넣는다.(공백 및 / 등 특수문자) 
        if REGEXP_SUBSTR(p_sso,'^Bugs ') is not null  then
                m_lenght:=length(p_sso);
                m_p_sso:=substr(p_sso,6,m_lenght) ;
                m_p_sso:=regexp_replace(m_p_sso,'(, )',',');
                --dbms_output.put_line(m_p_sso);  
        else
                --dbms_output.put_line('aaaa');
                m_p_sso:=regexp_replace(p_sso,'(, )',',');
        end if;
 
--bugs app 버전정보 넣기  =>Bugs를 잘라내고 첫글자부터 (가 나올때까지의 값을  가져온다 
        app_ver:=substr(m_p_sso,1,(instr(m_p_sso,'('))-2) ;
        m_p_sso:=substr(m_p_sso,(instr(m_p_sso,'('))+1,m_lenght);
        --dbms_output.put_line(app_ver||'test');
        --dbms_output.put_line(m_p_sso);
        

--패턴이 android나 windw면 그 이름 그대로 사용하고  ios관련이면 ios로 넣는다.
--각 os별루 패턴을 잘라낸다.
--android
        if REGEXP_SUBSTR(m_p_sso,'[Aa]ndroid') is not null then

                platform_nm:=REGEXP_SUBSTR(m_p_sso,'[Aa]ndroid');
                --dbms_output.put_line(platform_nm); 

                -- 뒷가로 )를 공백처리한다.
                m_p_sso:=REGEXP_REPLACE(m_p_sso,'[)]','');
                --dbms_output.put_line('공백'||m_p_sso); 

                --콤마를 구분자로 split후  나뉜 row들을 하나씩 불러온다.
                FOR REC IN (SELECT substr(wdata,
                                                        instr(wdata, ',', 1, LEVEL) + 1,
                                                        instr(wdata, ',', 1, LEVEL + 1) - instr(wdata, ',', 1, LEVEL) - 1)var 
                                     FROM ( SELECT ',' || m_p_sso|| ',' wdata FROM DUAL )
                                     CONNECT BY LEVEL <= length(wdata) - length(REPLACE(wdata, ',')) - 1)
                LOOP
                        case m_cnt 
                                when 1 then
                                        platform_ver:=substr(REGEXP_REPLACE(rec.var,'[Aa]ndroid ',''),1,20);
                                when 2 then
                                        device_nm:=substr(rec.var,1,50);
                                when 3 then
                                        comp_nm:=substr(rec.var,1,50);
                                when 4 then
                                        device_no:=substr(rec.var,1,50);
                                else
                                        null;
                        end case;
                m_cnt:=m_cnt+1;
                end loop;

        --/////////////  window
        elsif REGEXP_SUBSTR(m_p_sso,'[Ww]indows mobile') is not null then
                platform_nm:=REGEXP_SUBSTR(m_p_sso,'Windows mobile');
                --dbms_output.put_line(platform_nm); 
                
                platform_ver:=replace(REGEXP_SUBSTR(m_p_sso,'[0-9.]+\)'),')','');
                        
        --/////////////   IOS
        elsif REGEXP_SUBSTR(m_p_sso,'i[Pp]hone|i[Pp]od|i[Pp]ad') is not null then
                platform_nm:='iOS'; 
                --dbms_output.put_line(platform_nm);
                
                device_nm:=REGEXP_SUBSTR(m_p_sso,'i[Pp]hone|i[Pp]od|i[Pp]ad');
                --dbms_output.put_line(device_nm);
                
                comp_nm:='Apple';
                --dbms_output.put_line(comp_nm);               
                
                platform_ver:=replace(REGEXP_SUBSTR(m_p_sso,'[0-9.]+\)'),')','');                                
        else 
                null;
        end if;
END IF;

insert into tlg_login_log (msrl,sso_srl,login_ip1,login_tp,login_dt,widget_ref,m_app_ver,m_platform_nm,m_platform_ver,m_device_nm,m_comp_nm,m_device_no) 
                          values (P_MSRL,P_SSO_SRL,P_LOGIN_IP,P_LOGIN_TP,to_char(sysdate,'yyyymmddhh24miss'),P_WIDGET_REF,app_ver,platform_nm,platform_ver,device_nm,comp_nm,device_no);


END login_insert;

execute mobile_login_insert(2,3555555,'121.162.197.164','M',null,'Bugs 1.0.2 (iPhone Simulator ver 3.1.2)');
