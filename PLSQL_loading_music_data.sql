create or replace
PACKAGE BODY       global_job AS
PROCEDURE meta_sync(p_local_cd VARCHAR2, p_time_offset NUMBER)
IS

    CURSOR cur_artist(c_local_cd VARCHAR2, c_time_offset NUMBER) IS
        SELECT DISTINCT a.artist_id FROM ted_artist_w@cfeel a
        WHERE a.nation_cd = c_local_cd AND (a.crt_dt > trunc(SYSDATE + c_time_offset) OR a.upd_dt > trunc(SYSDATE + c_time_offset));
        
    CURSOR cur_artist_style (c_local_cd VARCHAR2, c_time_offset NUMBER) IS
        SELECT DISTINCT a.artist_id FROM tbm_artiststyle@cfeel a 
        WHERE crt_dt > trunc(SYSDATE + c_time_offset) AND EXISTS(SELECT 1 FROM ted_artist_w@cfeel b WHERE a.artist_id = b.artist_id AND b.nation_cd = c_local_cd);
        
    CURSOR cur_album (c_local_cd VARCHAR2, c_time_offset NUMBER) IS
        SELECT DISTINCT a.album_id FROM ted_album_w@cfeel a, ted_albumartist@cfeel b
        WHERE a.nation_cd = c_local_cd AND a.album_id = b.album_id
        AND (a.crt_dt > trunc(SYSDATE + c_time_offset) OR a.upd_dt > trunc(SYSDATE + c_time_offset) OR b.upd_dt > trunc(SYSDATE + c_time_offset));
        
    CURSOR cur_album_style (c_local_cd VARCHAR2, c_time_offset NUMBER) IS
        SELECT DISTINCT a.album_id FROM tbm_albumstyle@cfeel a
        WHERE crt_dt > trunc(SYSDATE + c_time_offset) AND EXISTS(SELECT 1 FROM ted_album_w@cfeel b WHERE a.album_id = b.album_id AND b.nation_cd = c_local_cd);
        
    CURSOR cur_track (c_local_cd VARCHAR2, c_time_offset NUMBER) IS
        SELECT DISTINCT a.TRACK_ID FROM ted_track_w@cfeel a, ted_trackartist@cfeel b
        WHERE a.nation_cd = c_local_cd AND a.track_id = b.track_id
        AND (a.crt_dt > trunc(SYSDATE + c_time_offset) OR a.upd_dt > trunc(SYSDATE + c_time_offset) OR b.upd_dt > trunc(SYSDATE + c_time_offset));
    
    CURSOR cur_mv (c_local_cd VARCHAR2, c_time_offset NUMBER) IS
        SELECT DISTINCT a.mv_id FROM ted_mv_w@cfeel a, ted_mvtrack@cfeel b
        WHERE a.nation_cd = 'VNM' AND a.mv_id = b.mv_id
        AND (a.crt_dt > trunc(SYSDATE + c_time_offset) OR a.upd_dt > trunc(SYSDATE + c_time_offset) OR b.upd_dt > trunc(SYSDATE + c_time_offset));
        
     
    m_local_cd VARCHAR2(10);        
    m_time_offset NUMBER;
    m_vnm_agency_id NUMBER := 19862;
    m_agency_id NUMBER;
BEGIN
    IF p_local_cd IS NULL THEN
        m_local_cd := 'VNM';
    ELSE 
        m_local_cd := p_local_cd;
    END IF;
    
    IF p_time_offset IS NULL THEN
        m_time_offset := -1;
    END IF;
    
    IF m_local_cd = 'VNM' THEN
        m_agency_id := m_vnm_agency_id;
    END IF;

    
    FOR rec_artist IN cur_artist(m_local_cd, m_time_offset) LOOP
    BEGIN
        MERGE INTO artist a
        USING (
            SELECT
                artist_id,artist_nm,disp_nm,search_nm,birth_ymd,nation_cd,grp_cd,sex_cd,
                (SELECT SITE_URL FROM tfm_website@CFEEL WHERE rel_entity_cd = 'ted_artist' and rel_id = src.artist_id) as homepage_url,
                act_start_ymd,act_end_ymd,crt_dt,upd_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_artist@cfeel src
            WHERE artist_id = rec_artist.artist_id
        ) b
        ON (a.artist_id = b.artist_id)
        WHEN MATCHED THEN
            UPDATE SET artist_nm = b.artist_nm, disp_nm = b.disp_nm, search_nm = b.search_nm, birth_ymd = b.birth_ymd,
                nation_cd = b.nation_cd, grp_cd = b.grp_cd, sex_cd = b.sex_cd, homepage_url = b.homepage_url, act_start_ymd = b.act_start_ymd,
                act_end_ymd = b.act_end_ymd, crt_dt = b.crt_dt, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (artist_id,artist_nm,disp_nm,search_nm,birth_ymd,nation_cd,grp_cd,sex_cd,homepage_url,act_start_ymd,act_end_ymd,crt_dt,status)
            VALUES (b.artist_id, b.artist_nm, b.disp_nm, b.search_nm, b.birth_ymd, b.nation_cd, b.grp_cd, b.sex_cd, 
            b.homepage_url, b.act_start_ymd, b.act_end_ymd, b.crt_dt, b.status);
        
        MERGE INTO artist_local a
        USING (
            SELECT 
               artist_id, artist_nm, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status, nation_cd as local_cd
            FROM ted_artist_w@cfeel
            WHERE artist_id = rec_artist.artist_id AND nation_cd = m_local_cd
        ) b
        ON (a.artist_id = b.artist_id and a.local_cd = b.local_cd)
        WHEN MATCHED THEN
            UPDATE SET disp_nm_local = b.artist_nm, search_nm_local = b.artist_nm, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (artist_id, local_cd, disp_nm_local, search_nm_local, crt_dt, status)
            VALUES (b.artist_id, b.local_cd, b.artist_nm, b.artist_nm, b.crt_dt, b.status);
    END;
    END LOOP;
    
    -- ARTIST STYLE
    FOR rec_artist_style IN cur_artist_style(m_local_cd, m_time_offset) LOOP
    BEGIN
        MERGE INTO artist_style a
        USING (
            SELECT artist_id, style_id, listorder, crt_dt
            FROM tbm_artiststyle@cfeel
            WHERE artist_id =rec_artist_style.artist_id
        ) b
        ON (a.artist_id = b.artist_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (artisT_id, style_id, listorder, crt_dt)
            VALUES(b.artist_id, b.style_id, b.listorder, sysdate);
    END;
    END LOOP;
    
    -- ALBUM
    FOR rec_album IN cur_album(m_local_cd, m_time_offset) LOOP
    BEGIN
        MERGE INTO album a
        USING (
            SELECT 
                album_id,title,search_title,
                (SELECt artist_id FROM ted_albumartist@cfeel ar 
                WHERE src.album_id = ar.album_id AND rp_cd = 'Y' AND db_sts = 'A' 
                AND EXISTS(SELECT 1 FROM artist bb WHERE ar.artist_id = bb.artist_id) AND ROWNUM = 1) as artist_id,
                nation_cd,release_ymd,album_type,crt_dt,upd_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_album@cfeel src
            WHERE album_id = rec_album.album_id
        ) b
        ON ( a.album_id = b.album_id)
        WHEN MATCHED THEN
            UPDATE SET title = b.title, search_title = b.search_title, artist_id = b.artist_id, nation_cd = b.nation_cd, release_ymd = b.release_ymd,
            keyword = b.search_title, album_tp = b.album_type, upd_dt = sysdate, status = b.status, agency_id = m_agency_id
        WHEN NOT MATCHED THEN
            INSERT (album_id,title,search_title,artist_id,nation_cd,release_ymd,keyword,album_tp,crt_dt,status, agency_id)
            VALUES (b.album_id, b.title, b.search_title, b.artist_id, b.nation_cd, b.release_ymd, b.search_title, b.album_type, b.crt_dt, b.status, m_agency_id);
        
        MERGE INTO album_local a
        USING (
            SELECT album_id, title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status, nation_cd as local_cd
            FROM ted_album_w@cfeel
            WHERE album_id = rec_album.album_id AND nation_cd = m_local_cd
        ) b
        ON (a.album_id = b.album_id and a.local_cd = b.local_cd)
        WHEN MATCHED THEN
            UPDATE SET title_local = b.title, search_title_local = b.title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (album_id, local_cd, title_local, search_title_local, crt_dt, status)
            VALUES (b.album_id, b.local_cd, b.title, b.title, b.crt_dt, b.status);
    END;
    END LOOP;
    
    -- TRACK
    FOR rec_track IN cur_track(m_local_cd, m_time_offset) LOOP
    BEGIN
        MERGE INTO track a
        USING (
            SELECT
                track_id,media_no,disc_id,track_no,track_title,NVL(title_yn, 'N') as title_yn,len,crt_dt,upd_dt,album_id,
                (SELECT ARTIST_ID FROM TED_TRACKARTIST@CFEEL ar 
                WHERE ar.TRACK_ID = src.TRACK_ID AND RP_CD = 'Y' AND ar.DB_STS = 'A' AND ROWNUM = 1
                AND EXISTS(SELECT 1 FROM artist bb WHERE ar.artist_id = bb.artist_id)) as artist_id,
                nvl(svc_128_yn, 'N') as svc_128_yn,nvl(svc_192_yn, 'N') as svc_192_yn,nvl(svc_320_yn, 'N') as svc_320_yn,
                nvl(svc_mmp3_yn, 'N') as svc_mmp3_yn, nvl(svc_flac_yn, 'N') as svc_flac_yn, nvl(svc_wave_yn, 'N') as svc_wave_yn, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_track@cfeel src
            WHERE track_id = rec_track.track_id
        ) b
        ON (a.track_id = b.track_id)
        WHEN MATCHED THEN
            UPDATE SET media_no = b.media_no, disc_id = b.disc_id, track_no = b.track_no, track_title = b.track_title, title_yn = b.title_yn, len = b.len, crt_dt = b.crt_dt,
                upd_dt = sysdate, album_id = b.album_id, artist_id = b.artist_id, svc_128_yn = b.svc_128_yn, svc_192_yn = b.svc_192_yn, svc_320_yn = b.svc_320_yn,
                svc_wma_yn = b.svc_mmp3_yn, svc_flac_yn = b.svc_flac_yn, svc_wave_yn = b.svc_wave_yn, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (track_id,media_no,disc_id,track_no,track_title,title_yn,len,crt_dt,
                album_id,artist_id,svc_128_yn,svc_192_yn,svc_320_yn,svc_wma_yn,svc_flac_yn,svc_wave_yn, status)
            VALUES (b.track_id, b.media_no, b.disc_id, b.track_no, b.track_title, b.title_yn, b.len, b.crt_dt,
                b.album_id, b.artist_id, b.svc_128_yn, b.svc_192_yn, b.svc_320_yn, b.svc_mmp3_yn, b.svc_flac_yn, b.svc_wave_yn, b.status);
                
        
        MERGE INTO track_local a
        USING (
            SELECT track_id, nation_cd as local_cd, track_title, crt_dt, 
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 715), 'N') AS str_yn,
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 714), 'N') AS dnl_yn,
            --NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 715), 'N') AS mv_str_yn,
            decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_track_w@cfeel aa
            WHERE track_id = rec_track.track_id and nation_cd = m_local_cd
        ) b
        ON (a.track_id = b.track_id AND a.local_cd = b.local_cd)
        WHEN MATCHED THEN
            UPDATE SET track_title_local = b.track_title, str_rights_yn = b.str_yn, dnl_rights_yn = b.dnl_yn, mv_str_rights_yn = b.str_yn,upd_dt = sysdate
        WHEN NOT MATCHED THEN
            INSERT (track_id, local_cd, track_title_local, crt_dt, status, str_rights_yn, dnl_rights_yn,mv_str_rights_yn)
            VALUES (b.track_id, b.local_cd, b.track_title, b.crt_dt, b.status, b.str_yn, b.dnl_yn, b.str_yn);
            
        --LYRICS_TP
        UPDATE track_local a
        SET lyrics_tp = 'T'
        WHERE track_id = rec_track.track_id
        AND EXISTS(SELECT 1 FROM time_lyrics@bugslyrics b WHERE a.track_id = b.track_id AND a.local_cd = b.local_cd);
        
        UPDATE track_local a
        SET lyrics_tp = 'N'
        WHERE track_id = rec_track.track_id
        AND EXISTS(SELECT 1 FROM normal_lyrics@bugslyrics b WHERE a.track_id = b.track_id AND a.local_cd = b.local_cd)
        AND (lyrics_tp is null or lyrics_tp != 'T');
        
    END;
    END LOOP;
    
    -- ALBUM & TRACK STYLE
    FOR rec_album_style IN cur_album_style (m_local_cd, m_time_offset) LOOP
    BEGIN
        MERGE INTO album_style a
        USING (
            SELECT album_id, style_id, listorder, crt_dt
            FROM tbm_albumstyle@cfeel
            WHERE album_id = rec_album_style.album_id
        ) b
        ON (a.album_id = b.album_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (album_id, style_id, listorder, crt_dt)
            VALUES (b.album_id, b.style_id, b.listorder, b.crt_dt);
            
        MERGE INTO track_style a
        USING (
            SELECT track_id, aa.style_id, aa.listorder, aa.crt_dt
            FROM tbm_albumstyle@cfeel aa, track bb
            WHERE aa.album_id = rec_album_style.album_id AND aa.album_id = bb.album_id
        ) b
        ON (a.track_id = b.track_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (track_id, style_id, listorder, crt_dt)
            VALUES (b.track_id, b.style_id, b.listorder, b.crt_dt);
    END;
    END LOOP;
    
    -- MV
    --dbms_output.put_line('MV');
    dbms_output.put_line(m_local_cd);
    dbms_output.put_line(m_time_offset);
    FOR rec_mv IN cur_mv (m_local_cd, m_time_offset) LOOP
    BEGIN
        
        MERGE INTO mv a
        USING (
            SELECT
                aa.mv_id,cc.track_id,cc.artist_id,aa.media_no,mv_title,nation_cd,attr_tp,high_yn,actor,aa.dscr,release_ymd,
                media_yn,aa.crt_dt,aa.upd_dt,decode(aa.db_sts, 'A', 'OK', 'BLIND') as status,
                svc_fullhd_yn,svc_hd_yn,svc_sd_yn,svc_mp4_yn
            FROM ted_mv@cfeel aa, 
                (SELECT mv_id ,track_id FROM 
                        (SELECT b.mv_id,b.track_id,a.attr_tp,b.mvtrack_id,ROW_NUMBER () OVER (PARTITION BY b.mv_id ORDER BY b.mvtrack_id ) as mv_rank
                        FROM ted_mv@cfeel a, ted_mvtrack@cfeel b
                        WHERE a.mv_id=b.mv_id)
                WHERE mv_rank=1)bb,
                track cc
            WHERE aa.mv_id = rec_mv.mv_id and aa.mv_id = bb.mv_id AND bb.track_id = cc.track_id 
            AND exists(SELECT 1 FROM track dd WHERE bb.track_id = dd.track_id)
        ) b
        ON (a.mv_id = b.mv_id)
        WHEN MATCHED THEN
            UPDATE SET track_id = b.track_id, artist_id = b.artist_id, media_no = b.media_no, mv_title = b.mv_title, nation_cd = b.nation_cd,
                attr_tp = b.attr_tp, highrate_yn = b.high_yn, actor = b.actor, dscr = b.dscr, release_ymd = b.release_ymd,
                media_yn = b.media_yn, crt_dt = b.crt_dt, upd_dt = sysdate, status = b.status,
                svc_fullhd_yn = b.svc_fullhd_yn, svc_hd_yn = b.svc_hd_yn, svc_sd_yn = b.svc_sd_yn, svc_mp4_yn = b.svc_mp4_yn
        WHEN NOT MATCHED THEN
            INSERT (mv_id,track_id,artist_id,media_no,mv_title,nation_cd,attr_tp,highrate_yn,actor,dscr,release_ymd,
                media_yn,crt_dt,upd_dt,status,svc_fullhd_yn,svc_hd_yn,svc_sd_yn,svc_mp4_yn)
            VALUES (b.mv_id, b.track_id, b.artist_id, b.media_no, b.mv_title, b.nation_cd, b.attr_tp, b.high_yn, b.actor, b.dscr, b.release_ymd,
                b.media_yn, b.crt_dt, b.upd_dt, b.status, b.svc_fullhd_yn, b.svc_hd_yn, b.svc_sd_yn, b.svc_mp4_yn);
        
        --dbms_output.put_line(rec_mv.mv_id);
        
        MERGE INTO mv_local a
        USING (
            SELECT aa.mv_id, nation_cd as local_cd, mv_title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_mv_w@cfeel aa
            WHERE aa.mv_id = rec_mv.mv_id AND aa.nation_cd = m_local_cd AND EXISTS(SELECT 1 FROM mv bb WHERE aa.mv_id = bb.mv_id)
        ) b
        ON (a.mv_id = b.mv_id AND a.local_cd = m_local_cd)
        WHEN MATCHED THEN
            UPDATE SET mv_title_local = b.mv_title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (mv_id, local_cd, mv_title_local, crt_dt, status)
            VALUES (b.mv_id, m_local_cd, b.mv_title, b.crt_dt, b.status);
    END;
    END LOOP;
    
    dbms_output.put_line(m_local_cd);
    BEGIN
        dbms_output.put_line(m_local_cd);
        --m_time_offset := -1;
        MERGE INTO mv_local a
        USING (
            SELECT aa.mv_id, nation_cd as local_cd, mv_title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_mv_w@cfeel aa
            WHERE 
                EXISTS(SELECT DISTINCT a.mv_id FROM ted_mv_w@cfeel a, ted_mvtrack@cfeel b
            WHERE a.nation_cd = m_local_cd AND a.mv_id = b.mv_id AND aa.mv_id = a.mv_id
            AND (a.crt_dt > trunc(SYSDATE + m_time_offset) OR a.upd_dt > trunc(SYSDATE + m_time_offset) OR b.upd_dt > trunc(SYSDATE + m_time_offset))) 
            AND aa.nation_cd = m_local_cd AND EXISTS(SELECT 1 FROM mv bb WHERE aa.mv_id = bb.mv_id)
        ) b
        ON (a.mv_id = b.mv_id AND a.local_cd = m_local_cd)
        WHEN MATCHED THEN
            UPDATE SET mv_title_local = b.mv_title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (mv_id, local_cd, mv_title_local, crt_dt, status)
            VALUES (b.mv_id, m_local_cd, b.mv_title, b.crt_dt, b.status);
    END;
END meta_sync;

PROCEDURE vnm_meta_sync(p_time_offset NUMBER)
IS
    m_time_offset NUMBER;
    m_agency_id NUMBER := 19862;
    
    --////////////ted_track_w에서의 수정일 및 생성일이  sysdate -1 보다 큰 track_id나 / ted_track에서의 수정일 및 생성일이  sysdate -1 보다 큰 track_id 를 커서로 생성한다. 
    CURSOR cur_track (c_time_offset NUMBER) IS
        SELECT DISTINCT a.TRACK_ID FROM ted_track_w@cfeel a, ted_trackartist@cfeel b
        WHERE a.nation_cd = 'VNM' AND a.track_id = b.track_id
        AND (
            (a.crt_dt > trunc(SYSDATE + c_time_offset) OR a.upd_dt > trunc(SYSDATE + c_time_offset) OR b.upd_dt > trunc(SYSDATE + c_time_offset))
            OR
            EXISTS(SELECT 1 FROM ted_track@cfeel c WHERE a.track_id = c.track_id AND (c.crt_dt > trunc(SYSDATE + c_time_offset) OR c.upd_dt > trunc(SYSDATE + c_time_offset)))
        );
BEGIN
    
    IF p_time_offset IS NULL THEN
        m_time_offset := -1;
    ELSE
        m_time_offset := p_time_offset;
    END IF;
    

    --GENRE
    BEGIN
        MERGE INTO genre a
        USING (
            SELECT genre_cd, genre_nm FROM tmu_genre@cfeel WHERE genre_cd = pgenre_cd
        ) b
        ON (a.genre_id = b.genre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.genre_nm = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (genre_id, genre_nm, crt_dt)
            VALUES (b.genre_cd, b.genre_nm, sysdate);

    END;


    --STYLE
    BEGIN
        MERGE INTO style a
        USING (
            SELECT genre_cd, pgenre_cd, genre_nm FROM tmu_genre@cfeel aa
            WHERE genre_cd != pgenre_cd
            AND EXISTS(SELECT 1 FROM genre bb WHERE aa.genre_cd = bb.genre_id)
        ) b
        ON (a.style_id = b.pgenre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.style_nm = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (style_id, genre_id, style_nm, crt_dt)
            VALUES (b.pgenre_cd, b.genre_cd, b.genre_nm, sysdate);
        /*  
        MERGE INTO style_local a
        USING (
            SELECT genre_cd, pgenre_cd, genre_nm, nation_cd FROM tmu_genre_w@cfeel aa
            WHERE genre_cd != pgenre_cd AND nation_cd = 'VNM'
            AND EXISTS(SELECT 1 FROM style bb WHERE aa.pgenre_cd = bb.style_id)
        ) b
        ON ( a.style_id = b.pgenre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.style_nm_local = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (style_id, local_cd, style_nm_local, crt_dt)
            VALUES (b.pgenre_cd, 'VNM', b.genre_nm, sysdate);
            
        */
    END;
    -- ARTIST
    --////////////아티스트 기본정보만 
    BEGIN
        MERGE INTO artist a
        USING (
            SELECT
                src.artist_id, src.artist_nm,disp_nm,search_nm,birth_ymd, src.nation_cd,grp_cd,sex_cd,
                (SELECT SITE_URL FROM tfm_website@CFEEL WHERE rel_entity_cd = 'ted_artist' and rel_id = src.artist_id) as homepage_url,
                act_start_ymd,act_end_ymd, src.crt_dt, a.upd_dt, case when a.db_sts||src.db_sts = 'AA' THEN 'OK' ELSE 'BLIND' END as status
            FROM ted_artist@cfeel src,ted_artist_w@cfeel a
            WHERE src.artist_id = a.artist_id AND a.nation_cd = 'VNM'
            AND ((a.crt_dt > trunc(SYSDATE + m_time_offset) OR a.upd_dt > trunc(SYSDATE + m_time_offset)) 
                OR (src.crt_dt > trunc(SYSDATE + m_time_offset) OR src.upd_dt > trunc(SYSDATE + m_time_offset))
            )
        ) b
        ON (a.artist_id = b.artist_id)
        WHEN MATCHED THEN
            UPDATE SET artist_nm = b.artist_nm, disp_nm = b.disp_nm, search_nm = b.search_nm, birth_ymd = b.birth_ymd,
                nation_cd = b.nation_cd, grp_cd = b.grp_cd, sex_cd = b.sex_cd, homepage_url = b.homepage_url, act_start_ymd = b.act_start_ymd,
                act_end_ymd = b.act_end_ymd, crt_dt = b.crt_dt, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (artist_id,artist_nm,disp_nm,search_nm,birth_ymd,nation_cd,grp_cd,sex_cd,homepage_url,act_start_ymd,act_end_ymd,crt_dt,status)
            VALUES (b.artist_id, b.artist_nm, b.disp_nm, b.search_nm, b.birth_ymd, b.nation_cd, b.grp_cd, b.sex_cd, 
            b.homepage_url, b.act_start_ymd, b.act_end_ymd, b.crt_dt, b.status);
        
        
        ---////////////아티스트의  서비스국가정보 및 해당국가에 서비스할지. 
        MERGE INTO artist_local a
        USING (
            SELECT 
               artist_id, artist_nm, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status, nation_cd as local_cd
            FROM ted_artist_w@cfeel a
            WHERE (nation_cd = 'VNM' AND (a.crt_dt > trunc(SYSDATE + m_time_offset) OR a.upd_dt > trunc(SYSDATE + m_time_offset)))
            AND EXISTS(SELECT 1 FROM artist bb WHERE a.artist_id = bb.artist_id)
        ) b
        ON (a.artist_id = b.artist_id and a.local_cd = 'VNM')
        WHEN MATCHED THEN
            UPDATE SET disp_nm_local = b.artist_nm, search_nm_local = b.artist_nm, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (artist_id, local_cd, disp_nm_local, search_nm_local, crt_dt, status)
            VALUES (b.artist_id, 'VNM', b.artist_nm, b.artist_nm, b.crt_dt, b.status);
    END;
    
    
    -- ARTIST STYLE
    BEGIN
        MERGE INTO artist_style a
        USING (
            SELECT artist_id, style_id, listorder, crt_dt
            FROM tbm_artiststyle@cfeel aa
            WHERE crt_dt > trunc(SYSDATE + m_time_offset) AND EXISTS(SELECT 1 FROM artist bb WHERE aa.artist_id = bb.artist_id)
            AND EXISTS(SELECT 1 FROM style cc WHERE aa.style_id = cc.style_id)
        ) b
        ON (a.artist_id = b.artist_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (artisT_id, style_id, listorder, crt_dt)
            VALUES(b.artist_id, b.style_id, b.listorder, sysdate);
    END;
    
    -- ALBUM
    --////////////앨범  기본정보만 
    BEGIN
        MERGE INTO album a
        USING (
            SELECT 
                src.album_id, src.title,search_title,
                (SELECt artist_id FROM ted_albumartist@cfeel ar 
                WHERE src.album_id = ar.album_id AND rp_cd = 'Y' AND db_sts = 'A' 
                AND EXISTS(SELECT 1 FROM artist bb WHERE ar.artist_id = bb.artist_id) AND ROWNUM = 1) as artist_id,
                src.nation_cd,release_ymd,album_type, src.crt_dt, aa.upd_dt, case when src.db_sts||aa.db_sts||nvl(src.svc_aprv_yn,'N') = 'AAY' THEN 'OK' ELSE 'BLIND' END as status
            FROM ted_album@cfeel src,ted_album_w@cfeel aa
            WHERE src.album_id = aa.album_id AND aa.nation_cd = 'VNM'
            AND (
                (aa.crt_dt > trunc(SYSDATE + m_time_offset) OR aa.upd_dt > trunc(SYSDATE + m_time_offset))
                OR
                (src.crt_dt > trunc(SYSDATE + m_time_offset) OR src.upd_dt > trunc(SYSDATE + m_time_offset))
                OR
                EXISTS (
                SELECT 1 FROM ted_albumartist@cfeel bb
                WHERE aa.album_id = bb.album_id
                AND (bb.crt_dt > trunc(SYSDATE + m_time_offset) OR bb.upd_dt > trunc(SYSDATE + m_time_offset))
                ) 
            )
           
        ) b
        ON ( a.album_id = b.album_id)
        WHEN MATCHED THEN
            UPDATE SET title = b.title, search_title = b.search_title, artist_id = b.artist_id, nation_cd = b.nation_cd, release_ymd = b.release_ymd,
            keyword = b.search_title, album_tp = b.album_type, upd_dt = sysdate, status = b.status, agency_id = m_agency_id
        WHEN NOT MATCHED THEN
            INSERT (album_id,title,search_title,artist_id,nation_cd,release_ymd,keyword,album_tp,crt_dt,status, agency_id)
            VALUES (b.album_id, b.title, b.search_title, b.artist_id, b.nation_cd, b.release_ymd, b.search_title, b.album_type, b.crt_dt, b.status, m_agency_id);
        
        --////////////앨범  서비스국가정보 및 해당국가에 서비스할지.  
        MERGE INTO album_local a
        USING (
            SELECT album_id, title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status, nation_cd as local_cd
            FROM ted_album_w@cfeel src
            WHERE (((src.crt_dt > trunc(SYSDATE + m_time_offset) OR src.upd_dt > trunc(SYSDATE + m_time_offset)))
            OR EXISTS (
                SELECT 1 FROM ted_albumartist@cfeel bb
                WHERE src.album_id = bb.album_id
                AND bb.upd_dt > trunc(SYSDATE + m_time_offset)                
            )
            ) AND src.nation_cd = 'VNM' AND EXISTS(SELECT 1 FROM album dd WHERE src.album_id = dd.album_id)
        ) b
        ON (a.album_id = b.album_id and a.local_cd = 'VNM')
        WHEN MATCHED THEN
            UPDATE SET title_local = b.title, search_title_local = b.title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (album_id, local_cd, title_local, search_title_local, crt_dt, status)
            VALUES (b.album_id, 'VNM', b.title, b.title, b.crt_dt, b.status);
    END;
    
    -- TRACK
    FOR rec_track IN cur_track(m_time_offset) LOOP
    BEGIN
    
     --////////////트랙  기본정보만
        MERGE INTO track a
        USING (
            SELECT
                src.track_id,media_no,disc_id,track_no, src.track_title,NVL(title_yn, 'N') as title_yn,len, src.crt_dt, a.upd_dt,album_id,
                (SELECT ARTIST_ID FROM TED_TRACKARTIST@CFEEL ar 
                WHERE ar.TRACK_ID = src.TRACK_ID AND RP_CD = 'Y' AND ar.DB_STS = 'A' AND ROWNUM = 1
                AND EXISTS(SELECT 1 FROM artist bb WHERE ar.artist_id = bb.artist_id)) as artist_id,
                nvl(svc_128_yn, 'N') as svc_128_yn,nvl(svc_192_yn, 'N') as svc_192_yn,nvl(svc_320_yn, 'N') as svc_320_yn,
                nvl(svc_mmp3_yn, 'N') as svc_mmp3_yn, nvl(svc_flac_yn, 'N') as svc_flac_yn, nvl(svc_wave_yn, 'N') as svc_wave_yn, 
                CASE WHEN src.db_sts||a.db_sts = 'AA' THEN 'OK' ELSE 'BLIND' END as status
            FROM ted_track@cfeel src, ted_track_w@cfeel a
            WHERE src.track_id = rec_track.track_id AND src.track_id = a.track_id and a.nation_cd = 'VNM'
            AND EXISTS(SELECT 1 FROM album c WHERE src.album_id = c.album_id)
        ) b
        ON (a.track_id = b.track_id)
        WHEN MATCHED THEN
            UPDATE SET media_no = b.media_no, disc_id = b.disc_id, track_no = b.track_no, track_title = b.track_title, title_yn = b.title_yn, len = b.len, crt_dt = b.crt_dt,
                upd_dt = sysdate, album_id = b.album_id, artist_id = b.artist_id, svc_128_yn = b.svc_128_yn, svc_192_yn = b.svc_192_yn, svc_320_yn = b.svc_320_yn,
                svc_wma_yn = b.svc_mmp3_yn, svc_flac_yn = b.svc_flac_yn, svc_wave_yn = b.svc_wave_yn, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (track_id,media_no,disc_id,track_no,track_title,title_yn,len,crt_dt,
                album_id,artist_id,svc_128_yn,svc_192_yn,svc_320_yn,svc_wma_yn,svc_flac_yn,svc_wave_yn, status)
            VALUES (b.track_id, b.media_no, b.disc_id, b.track_no, b.track_title, b.title_yn, b.len, b.crt_dt,
                b.album_id, b.artist_id, b.svc_128_yn, b.svc_192_yn, b.svc_320_yn, b.svc_mmp3_yn, b.svc_flac_yn, b.svc_wave_yn, b.status);
                
                
        --////////////트랙   서비스국가정보 및 해당국가에 서비스할지.  및 권리 
        MERGE INTO track_local a
        USING (
            SELECT track_id, nation_cd as local_cd, track_title, crt_dt, 
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 715), 'N') AS str_yn,
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 714), 'N') AS dnl_yn,
            --NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 715), 'N') AS mv_str_yn,
            decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_track_w@cfeel aa
            WHERE track_id = rec_track.track_id and nation_cd = 'VNM'
            AND EXISTS(SELECT 1 FROM track c WHERE aa.track_id = c.track_id)
        ) b
        ON (a.track_id = b.track_id AND a.local_cd = 'VNM')
        WHEN MATCHED THEN
            UPDATE SET track_title_local = b.track_title, str_rights_yn = b.str_yn, dnl_rights_yn = b.dnl_yn, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (track_id, local_cd, track_title_local, crt_dt, status, str_rights_yn, dnl_rights_yn,mv_str_rights_yn)
            VALUES (b.track_id, 'VNM', b.track_title, b.crt_dt, b.status, b.str_yn, b.dnl_yn, b.str_yn);
            
        --LYRICS_TP
        UPDATE track_local a
        SET lyrics_tp = 'T'
        WHERE track_id = rec_track.track_id
        AND EXISTS(SELECT 1 FROM time_lyrics@bugslyrics b WHERE a.track_id = b.track_id AND a.local_cd = b.local_cd);
        
        UPDATE track_local a
        SET lyrics_tp = 'N'
        WHERE track_id = rec_track.track_id
        AND EXISTS(SELECT 1 FROM normal_lyrics@bugslyrics b WHERE a.track_id = b.track_id AND a.local_cd = b.local_cd)
        AND (lyrics_tp is null or lyrics_tp != 'T');
        
    END;
    END LOOP;
    
    -- ALBUM & TRACK STYLE
    BEGIN
        MERGE INTO album_style a
        USING (
            SELECT album_id, style_id, listorder, crt_dt
            FROM tbm_albumstyle@cfeel aa
            WHERE crt_dt > trunc(SYSDATE + m_time_offset)
            AND EXISTS (SELECT 1 FROM album bb WHERE aa.album_id = bb.album_id)
            AND EXISTS (SELECT 1 FROM style bb WHERE aa.style_id = bb.style_id)
           
        ) b
        ON (a.album_id = b.album_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (album_id, style_id, listorder, crt_dt)
            VALUES (b.album_id, b.style_id, b.listorder, b.crt_dt);
            
        MERGE INTO track_style a
        USING (
            SELECT track_id, aa.style_id, aa.listorder, aa.crt_dt
            FROM tbm_albumstyle@cfeel aa, track bb
            WHERE aa.crt_dt > trunc(SYSDATE + m_time_offset)
            AND aa.album_id = bb.album_id
            AND EXISTS (SELECT 1 FROM style cc WHERE aa.style_id = cc.style_id)
        ) b
        ON (a.track_id = b.track_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (track_id, style_id, listorder, crt_dt)
            VALUES (b.track_id, b.style_id, b.listorder, b.crt_dt);
    END;
    
    -- MV
    --dbms_output.put_line('MV');
    dbms_output.put_line(m_time_offset);
    BEGIN
    
    --///////////ted_mvtrack에서 db_sts가 D이면 해당 mv는 BLIND처리한다. 
        UPDATE wmeta.mv a
        SET status = 'BLIND', upd_dt = SYSDATE
        WHERE EXISTS(SELECT 1 FROM ted_mvtrack@cfeel b 
            WHERE a.mv_id = b.mv_id AND a.track_id = b.track_id AND b.db_sts = 'D' AND b.upd_dt > trunc(SYSDATE + m_time_offset));

     --////////////뮤비   기본정보만        
        MERGE INTO mv a
        USING (
            SELECT
                aa.mv_id,cc.track_id,cc.artist_id,aa.media_no, aa.mv_title, aa.nation_cd,attr_tp,high_yn,actor,aa.dscr,release_ymd,
                media_yn,aa.crt_dt, dd.upd_dt, CASE WHEN aa.db_sts||dd.db_sts||nvl(aa.svc_aprv_yn,'N') = 'AAY' THEN 'OK' ELSE 'BLIND' END as status,
                svc_fullhd_yn,svc_hd_yn,svc_sd_yn,svc_mp4_yn
            FROM ted_mv@cfeel aa,
                (SELECT mv_id ,track_id FROM 
                        (SELECT b.mv_id,b.track_id,a.attr_tp,b.mvtrack_id,ROW_NUMBER () OVER (PARTITION BY b.mv_id ORDER BY b.mvtrack_id ) as mv_rank
                        FROM ted_mv@cfeel a, ted_mvtrack@cfeel b
                        WHERE a.mv_id=b.mv_id and b.db_sts ='A')
                WHERE mv_rank=1)bb,
                track cc, ted_mv_w@cfeel dd
            WHERE aa.mv_id = bb.mv_id AND bb.track_id = cc.track_id
            AND aa.mv_id = dd.mv_id and dd.nation_cd = 'VNM'
            AND (
                EXISTS (
                    SELECT 1 FROM ted_mvtrack@cfeel ee
                    WHERE dd.mv_id = ee.mv_id
                    AND ee.upd_dt > trunc(SYSDATE + m_time_offset)
                )
                OR (dd.crt_dt > trunc(SYSDATE + m_time_offset) OR dd.upd_dt > trunc(SYSDATE + m_time_offset))
                OR (aa.crt_dt > trunc(SYSDATE + m_time_offset) OR aa.upd_dt > trunc(SYSDATE + m_time_offset))
            )
        ) b
        ON (a.mv_id = b.mv_id)
        WHEN MATCHED THEN
            UPDATE SET track_id = b.track_id, artist_id = b.artist_id, media_no = b.media_no, mv_title = b.mv_title, nation_cd = b.nation_cd,
                attr_tp = b.attr_tp, highrate_yn = b.high_yn, actor = b.actor, dscr = b.dscr, release_ymd = b.release_ymd,
                media_yn = b.media_yn, crt_dt = b.crt_dt, upd_dt = sysdate, status = b.status,
                svc_fullhd_yn = b.svc_fullhd_yn, svc_hd_yn = b.svc_hd_yn, svc_sd_yn = b.svc_sd_yn, svc_mp4_yn = b.svc_mp4_yn
        WHEN NOT MATCHED THEN
            INSERT (mv_id,track_id,artist_id,media_no,mv_title,nation_cd,attr_tp,highrate_yn,actor,dscr,release_ymd,
                media_yn,crt_dt,upd_dt,status,svc_fullhd_yn,svc_hd_yn,svc_sd_yn,svc_mp4_yn)
            VALUES (b.mv_id, b.track_id, b.artist_id, b.media_no, b.mv_title, b.nation_cd, b.attr_tp, b.high_yn, b.actor, b.dscr, b.release_ymd,
                b.media_yn, b.crt_dt, b.upd_dt, b.status, b.svc_fullhd_yn, b.svc_hd_yn, b.svc_sd_yn, b.svc_mp4_yn);
        
        --dbms_output.put_line(rec_mv.mv_id);
 
 
      --////////////뮤비   서비스국가정보 및 해당국가에 서비스할지.  및 권리            
        MERGE INTO mv_local a
        USING (
            SELECT aa.mv_id, nation_cd as local_cd, mv_title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_mv_w@cfeel aa
            WHERE ((aa.nation_cd = 'VNM' AND (aa.crt_dt > trunc(SYSDATE + m_time_offset) OR aa.upd_dt > trunc(SYSDATE + m_time_offset)))
                        
            )AND EXISTS(SELECT 1 FROM mv bb WHERE aa.mv_id = bb.mv_id)
        ) b
        ON (a.mv_id = b.mv_id AND a.local_cd = 'VNM')
        WHEN MATCHED THEN
            UPDATE SET mv_title_local = b.mv_title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (mv_id, local_cd, mv_title_local, crt_dt, status)
            VALUES (b.mv_id, 'VNM', b.mv_title, b.crt_dt, b.status);
    END;
    
    --///////권리 테이블에서 해당 앨범의 다운로드 스트리밍 권리가 한개도 없으면 BLIND처리한다. 
    BEGIN
        UPDATE album_local a
        SET status = 'BLIND', upd_dt = sysdate
        WHERE NOT EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 715 AND right_yn = 'Y') OR (service_id = 714 AND right_yn = 'Y')))
        AND local_cd = 'VNM';
            
        UPDATE album_local a
        SET status = 'OK', upd_dt = sysdate
        WHERE EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 715 AND right_yn = 'Y') OR (service_id = 714 AND right_yn = 'Y')))
        AND status != 'OK'
        AND local_cd = 'VNM';
    END;

END vnm_meta_sync;

PROCEDURE vnm_rights_sync(p_time_offset NUMBER)
IS
    m_str_svc_id NUMBER := 715;
    m_dnl_svc_id NUMBER := 714;    
    m_target_dt DATE := trunc(sysdate -1);
BEGIN

    IF p_time_offset IS NOT NULL THEN 
        m_target_dt := trunc(sysdate + p_time_offset);
    ELSE
        m_target_dt := trunc(sysdate -1);
    END IF;
    
    -- STR RIGHTS
    UPDATE track_local a
    SET (str_rights_yn, mv_str_rights_yn) = 
        (SELECT nvl(right_yn, 'N'), nvl(right_yn, 'N') FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_str_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)), upd_dt = sysdate
    WHERE 
        local_cd = 'VNM' AND
        EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_str_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)
        );
    
    -- DNL RIGHTS
    UPDATE track_local a
    SET (dnl_rights_yn) = 
        (SELECT nvl(right_yn, 'N') FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_dnl_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)), upd_dt = sysdate
    WHERE 
        local_cd = 'VNM' AND
        EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_dnl_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)
        );

  -----2010.10.10. colasarang--추가---      
    BEGIN
        UPDATE album_local a
        SET status = 'BLIND', upd_dt = sysdate
        WHERE NOT EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 715 AND right_yn = 'Y') OR (service_id = 714 AND right_yn = 'Y')))
        AND local_cd = 'VNM';
            
        UPDATE album_local a
        SET status = 'OK', upd_dt = sysdate
        WHERE EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 715 AND right_yn = 'Y') OR (service_id = 714 AND right_yn = 'Y')))
        AND status != 'OK'
        AND local_cd = 'VNM';
    END;

END vnm_rights_sync;


PROCEDURE jpn_meta_sync(p_time_offset NUMBER)
IS
--#################################################################
--##########반드시   service_id와 agency_id확인하기#############$$$$$$$$$$$$$$$$$------
--#################################################################
    m_time_offset NUMBER;
    m_agency_id NUMBER := 20382; 
    
    CURSOR cur_track (c_time_offset NUMBER) IS
        SELECT DISTINCT a.TRACK_ID FROM ted_track_w@cfeel a, ted_trackartist@cfeel b  -- TODO ted_trackartist outer ?
        WHERE a.nation_cd = 'JPN' AND a.track_id = b.track_id
        AND (
            (a.crt_dt > trunc(SYSDATE + c_time_offset) OR a.upd_dt > trunc(SYSDATE + c_time_offset) OR b.upd_dt > trunc(SYSDATE + c_time_offset))
            OR
            EXISTS(SELECT 1 FROM ted_track@cfeel c WHERE a.track_id = c.track_id AND (c.crt_dt > trunc(SYSDATE + c_time_offset) OR c.upd_dt > trunc(SYSDATE + c_time_offset)))
        );
BEGIN
    
    IF p_time_offset IS NULL THEN
        m_time_offset := -1;
    ELSE
        m_time_offset := p_time_offset;
    END IF;
    
    
    --AGENCY 20382 일본지사 고정 
    
    --GENRE
    BEGIN
        MERGE INTO genre a
        USING (
            SELECT genre_cd, genre_nm FROM tmu_genre@cfeel WHERE genre_cd = pgenre_cd
        ) b
        ON (a.genre_id = b.genre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.genre_nm = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (genre_id, genre_nm, crt_dt)
            VALUES (b.genre_cd, b.genre_nm, sysdate);
        /*     
        MERGE INTO genre_local a
        USING (
            SELECT genre_cd, genre_nm, nation_cd FROM tmu_genre_w@cfeel WHERE genre_cd = pgenre_cd AND nation_cd = 'JPN'
        ) b
        ON ( a.genre_id = b.genre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.genre_nm_local = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (genre_id, local_cd, genre_nm_local, crt_dt)
            VALUES (b.genre_cd, 'JPN', b.genre_nm, sysdate);
        */
    END;


    --STYLE
    BEGIN
        MERGE INTO style a
        USING (
            SELECT genre_cd, pgenre_cd, genre_nm FROM tmu_genre@cfeel aa
            WHERE genre_cd != pgenre_cd
            AND EXISTS(SELECT 1 FROM genre bb WHERE aa.genre_cd = bb.genre_id)
        ) b
        ON (a.style_id = b.pgenre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.style_nm = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (style_id, genre_id, style_nm, crt_dt)
            VALUES (b.pgenre_cd, b.genre_cd, b.genre_nm, sysdate);
        /*  
        MERGE INTO style_local a
        USING (
            SELECT genre_cd, pgenre_cd, genre_nm, nation_cd FROM tmu_genre_w@cfeel aa
            WHERE genre_cd != pgenre_cd AND nation_cd = 'JPN'
            AND EXISTS(SELECT 1 FROM style bb WHERE aa.pgenre_cd = bb.style_id)
        ) b
        ON ( a.style_id = b.pgenre_cd)
        WHEN MATCHED THEN
            UPDATE SET a.style_nm_local = b.genre_nm
        WHEN NOT MATCHED THEN
            INSERT (style_id, local_cd, style_nm_local, crt_dt)
            VALUES (b.pgenre_cd, 'JPN', b.genre_nm, sysdate);
        */
    END;
    
    -- ARTIST
    
    BEGIN
        MERGE INTO artist a
        USING (
            SELECT
                src.artist_id, src.artist_nm,disp_nm,search_nm,birth_ymd, src.nation_cd,grp_cd,sex_cd,
                (SELECT SITE_URL FROM tfm_website@CFEEL WHERE rel_entity_cd = 'ted_artist' and rel_id = src.artist_id) as homepage_url,
                act_start_ymd,act_end_ymd, src.crt_dt, a.upd_dt, case when a.db_sts||src.db_sts = 'AA' THEN 'OK' ELSE 'BLIND' END as status
            FROM ted_artist@cfeel src,ted_artist_w@cfeel a
            WHERE src.artist_id = a.artist_id AND a.nation_cd = 'JPN'
            AND ((a.crt_dt > trunc(SYSDATE + m_time_offset) OR a.upd_dt > trunc(SYSDATE + m_time_offset)) 
                OR (src.crt_dt > trunc(SYSDATE + m_time_offset) OR src.upd_dt > trunc(SYSDATE + m_time_offset))
            )
        ) b
        ON (a.artist_id = b.artist_id)
        WHEN MATCHED THEN
            UPDATE SET artist_nm = b.artist_nm, disp_nm = b.disp_nm, search_nm = b.search_nm, birth_ymd = b.birth_ymd,
                nation_cd = b.nation_cd, grp_cd = b.grp_cd, sex_cd = b.sex_cd, homepage_url = b.homepage_url, act_start_ymd = b.act_start_ymd,
                act_end_ymd = b.act_end_ymd, crt_dt = b.crt_dt, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (artist_id,artist_nm,disp_nm,search_nm,birth_ymd,nation_cd,grp_cd,sex_cd,homepage_url,act_start_ymd,act_end_ymd,crt_dt,status)
            VALUES (b.artist_id, b.artist_nm, b.disp_nm, b.search_nm, b.birth_ymd, b.nation_cd, b.grp_cd, b.sex_cd, 
            b.homepage_url, b.act_start_ymd, b.act_end_ymd, b.crt_dt, b.status);
        
        MERGE INTO artist_local a
        USING (
            SELECT 
               artist_id, artist_nm, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status, nation_cd as local_cd
            FROM ted_artist_w@cfeel a
            WHERE (nation_cd = 'JPN' AND (a.crt_dt > trunc(SYSDATE + m_time_offset) OR a.upd_dt > trunc(SYSDATE + m_time_offset)))
            AND EXISTS(SELECT 1 FROM artist bb WHERE a.artist_id = bb.artist_id)
        ) b
        ON (a.artist_id = b.artist_id and a.local_cd = 'JPN')
        WHEN MATCHED THEN
            UPDATE SET disp_nm_local = b.artist_nm, search_nm_local = b.artist_nm, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (artist_id, local_cd, disp_nm_local, search_nm_local, crt_dt, status)
            VALUES (b.artist_id, 'JPN', b.artist_nm, b.artist_nm, b.crt_dt, b.status);
    END;
    
    
    -- ARTIST STYLE
    BEGIN
        MERGE INTO artist_style a
        USING (
            SELECT artist_id, style_id, listorder, crt_dt
            FROM tbm_artiststyle@cfeel aa
            WHERE crt_dt > trunc(SYSDATE + m_time_offset) AND EXISTS(SELECT 1 FROM artist bb WHERE aa.artist_id = bb.artist_id)
            AND EXISTS(SELECT 1 FROM style cc WHERE aa.style_id = cc.style_id)
        ) b
        ON (a.artist_id = b.artist_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (artisT_id, style_id, listorder, crt_dt)
            VALUES(b.artist_id, b.style_id, b.listorder, sysdate);
    END;
    
    -- ALBUM
    BEGIN
        MERGE INTO album a
        USING (
            SELECT 
                src.album_id, src.title,search_title,
                (SELECt artist_id FROM ted_albumartist@cfeel ar 
                WHERE src.album_id = ar.album_id AND rp_cd = 'Y' AND db_sts = 'A' 
                AND EXISTS(SELECT 1 FROM artist bb WHERE ar.artist_id = bb.artist_id) AND ROWNUM = 1) as artist_id,
                src.nation_cd,release_ymd,album_type, src.crt_dt, aa.upd_dt, case when src.db_sts||aa.db_sts||nvl(src.svc_aprv_yn,'N') = 'AAY' THEN 'OK' ELSE 'BLIND' END as status
            FROM ted_album@cfeel src,ted_album_w@cfeel aa
            WHERE src.album_id = aa.album_id AND aa.nation_cd = 'JPN'
            AND (
                (aa.crt_dt > trunc(SYSDATE + m_time_offset) OR aa.upd_dt > trunc(SYSDATE + m_time_offset))
                OR
                (src.crt_dt > trunc(SYSDATE + m_time_offset) OR src.upd_dt > trunc(SYSDATE + m_time_offset))
                OR
                EXISTS (
                SELECT 1 FROM ted_albumartist@cfeel bb
                WHERE aa.album_id = bb.album_id
                AND (bb.crt_dt > trunc(SYSDATE + m_time_offset) OR bb.upd_dt > trunc(SYSDATE + m_time_offset))
                ) 
            )
           
        ) b
        ON ( a.album_id = b.album_id)
        WHEN MATCHED THEN
            UPDATE SET title = b.title, search_title = b.search_title, artist_id = b.artist_id, nation_cd = b.nation_cd, release_ymd = b.release_ymd,
            keyword = b.search_title, album_tp = b.album_type, upd_dt = sysdate, status = b.status, agency_id = m_agency_id
        WHEN NOT MATCHED THEN
            INSERT (album_id,title,search_title,artist_id,nation_cd,release_ymd,keyword,album_tp,crt_dt,status, agency_id)
            VALUES (b.album_id, b.title, b.search_title, b.artist_id, b.nation_cd, b.release_ymd, b.search_title, b.album_type, b.crt_dt, b.status, m_agency_id);
        
        MERGE INTO album_local a
        USING (
            SELECT album_id, title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status, nation_cd as local_cd
            FROM ted_album_w@cfeel src
            WHERE (( (src.crt_dt > trunc(SYSDATE + m_time_offset) OR src.upd_dt > trunc(SYSDATE + m_time_offset)))
            OR EXISTS (
                SELECT 1 FROM ted_albumartist@cfeel bb
                WHERE src.album_id = bb.album_id
                AND bb.upd_dt > trunc(SYSDATE + m_time_offset)                
            )
            ) AND src.nation_cd = 'JPN'AND EXISTS(SELECT 1 FROM album dd WHERE src.album_id = dd.album_id)
        ) b
        ON (a.album_id = b.album_id and a.local_cd = 'JPN')
        WHEN MATCHED THEN
            UPDATE SET title_local = b.title, search_title_local = b.title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (album_id, local_cd, title_local, search_title_local, crt_dt, status)
            VALUES (b.album_id, 'JPN', b.title, b.title, b.crt_dt, b.status);
    END;
    
    -- TRACK
    FOR rec_track IN cur_track(m_time_offset) LOOP
    BEGIN
        MERGE INTO track a
        USING (
            SELECT
                src.track_id,media_no,disc_id,track_no, src.track_title,NVL(title_yn, 'N') as title_yn,len, src.crt_dt, a.upd_dt,album_id,
                (SELECT ARTIST_ID FROM TED_TRACKARTIST@CFEEL ar 
                WHERE ar.TRACK_ID = src.TRACK_ID AND RP_CD = 'Y' AND ar.DB_STS = 'A' AND ROWNUM = 1
                AND EXISTS(SELECT 1 FROM artist bb WHERE ar.artist_id = bb.artist_id)) as artist_id,
                nvl(svc_128_yn, 'N') as svc_128_yn,nvl(svc_192_yn, 'N') as svc_192_yn,nvl(svc_320_yn, 'N') as svc_320_yn,
                nvl(svc_mmp3_yn, 'N') as svc_mmp3_yn, nvl(svc_flac_yn, 'N') as svc_flac_yn, nvl(svc_wave_yn, 'N') as svc_wave_yn, 
                CASE WHEN src.db_sts||a.db_sts = 'AA' THEN 'OK' ELSE 'BLIND' END as status
            FROM ted_track@cfeel src, ted_track_w@cfeel a
            WHERE src.track_id = rec_track.track_id AND src.track_id = a.track_id AND a.nation_cd = 'JPN'
            AND EXISTS(SELECT 1 FROM album c WHERE src.album_id = c.album_id)
        ) b
        ON (a.track_id = b.track_id)
        WHEN MATCHED THEN
            UPDATE SET media_no = b.media_no, disc_id = b.disc_id, track_no = b.track_no, track_title = b.track_title, title_yn = b.title_yn, len = b.len, crt_dt = b.crt_dt,
                upd_dt = sysdate, album_id = b.album_id, artist_id = b.artist_id, svc_128_yn = b.svc_128_yn, svc_192_yn = b.svc_192_yn, svc_320_yn = b.svc_320_yn,
                svc_wma_yn = b.svc_mmp3_yn, svc_flac_yn = b.svc_flac_yn, svc_wave_yn = b.svc_wave_yn, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (track_id,media_no,disc_id,track_no,track_title,title_yn,len,crt_dt,
                album_id,artist_id,svc_128_yn,svc_192_yn,svc_320_yn,svc_wma_yn,svc_flac_yn,svc_wave_yn, status)
            VALUES (b.track_id, b.media_no, b.disc_id, b.track_no, b.track_title, b.title_yn, b.len, b.crt_dt,
                b.album_id, b.artist_id, b.svc_128_yn, b.svc_192_yn, b.svc_320_yn, b.svc_mmp3_yn, b.svc_flac_yn, b.svc_wave_yn, b.status);
                
        
        MERGE INTO track_local a
        USING (
            SELECT track_id, nation_cd as local_cd, track_title, crt_dt, 
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 716), 'N') AS str_yn,
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 717), 'N') AS dnl_yn,
            NVL((SELECT right_yn FROM ttmp_trackright@cfeel bb WHERE aa.track_id = bb.track_id AND service_id = 718), 'N') AS rent_yn,
            decode(db_sts, 'A', 'OK', 'BLIND') as status, track_title_alt, search_title
            FROM ted_track_w@cfeel aa
            WHERE track_id = rec_track.track_id and nation_cd = 'JPN'
            AND EXISTS(SELECT 1 FROM track c WHERE aa.track_id = c.track_id)
        ) b
        ON (a.track_id = b.track_id AND a.local_cd = 'JPN')
        WHEN MATCHED THEN
            UPDATE SET track_title_local = b.track_title, str_rights_yn = b.str_yn, dnl_rights_yn = b.dnl_yn, rent_rights_yn = b.rent_yn, upd_dt = sysdate, status = b.status,
                    track_title_alt = b.track_title_alt, search_title = b.search_title, mv_str_rights_yn = b.str_yn
        WHEN NOT MATCHED THEN
            INSERT (track_id, local_cd, track_title_local, crt_dt, status, str_rights_yn, dnl_rights_yn,mv_str_rights_yn, rent_rights_yn, track_title_alt, search_title)
            VALUES (b.track_id, 'JPN', b.track_title, b.crt_dt, b.status, b.str_yn, b.dnl_yn, b.str_yn, b.rent_yn, b.track_title_alt, b.search_title);
            
        --LYRICS_TP
        UPDATE track_local a
        SET lyrics_tp = 'T'
        WHERE track_id = rec_track.track_id
        AND EXISTS(SELECT 1 FROM time_lyrics@bugslyrics b WHERE a.track_id = b.track_id AND a.local_cd = b.local_cd);
        
        UPDATE track_local a
        SET lyrics_tp = 'N'
        WHERE track_id = rec_track.track_id
        AND EXISTS(SELECT 1 FROM normal_lyrics@bugslyrics b WHERE a.track_id = b.track_id AND a.local_cd = b.local_cd)
        AND (lyrics_tp is null or lyrics_tp != 'T');
        
    END;
    END LOOP;
    
    -- ALBUM & TRACK STYLE
    BEGIN
        MERGE INTO album_style a
        USING (
            SELECT album_id, style_id, listorder, crt_dt
            FROM tbm_albumstyle@cfeel aa
            WHERE crt_dt > trunc(SYSDATE + m_time_offset)
            AND EXISTS (SELECT 1 FROM album bb WHERE aa.album_id = bb.album_id)
            AND EXISTS (SELECT 1 FROM style bb WHERE aa.style_id = bb.style_id)
           
        ) b
        ON (a.album_id = b.album_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (album_id, style_id, listorder, crt_dt)
            VALUES (b.album_id, b.style_id, b.listorder, b.crt_dt);
            
        MERGE INTO track_style a
        USING (
            SELECT track_id, aa.style_id, aa.listorder, aa.crt_dt
            FROM tbm_albumstyle@cfeel aa, track bb
            WHERE aa.crt_dt > trunc(SYSDATE + m_time_offset)
            AND aa.album_id = bb.album_id
            AND EXISTS (SELECT 1 FROM style cc WHERE aa.style_id = cc.style_id)
        ) b
        ON (a.track_id = b.track_id AND a.style_id = b.style_id)
        WHEN MATCHED THEN
            UPDATE SET listorder = b.listorder
        WHEN NOT MATCHED THEN
            INSERT (track_id, style_id, listorder, crt_dt)
            VALUES (b.track_id, b.style_id, b.listorder, b.crt_dt);
    END;
    
    -- MV
    --dbms_output.put_line('MV');
    dbms_output.put_line(m_time_offset);
    BEGIN
        UPDATE wmeta.mv a
        SET status = 'BLIND', upd_dt = SYSDATE
        WHERE EXISTS(SELECT 1 FROM ted_mvtrack@cfeel b 
            WHERE a.mv_id = b.mv_id AND a.track_id = b.track_id AND b.db_sts = 'D' AND b.upd_dt > trunc(SYSDATE + m_time_offset));
        
        MERGE INTO mv a
        USING (
            SELECT
                aa.mv_id,cc.track_id,cc.artist_id,aa.media_no, aa.mv_title, aa.nation_cd,attr_tp,high_yn,actor,aa.dscr,release_ymd,
                media_yn,aa.crt_dt, dd.upd_dt, CASE WHEN aa.db_sts||dd.db_sts||nvl(aa.svc_aprv_yn,'N') = 'AAY' THEN 'OK' ELSE 'BLIND' END as status,
                svc_fullhd_yn,svc_hd_yn,svc_sd_yn,svc_mp4_yn
            FROM ted_mv@cfeel aa,
                (SELECT mv_id ,track_id FROM 
                        (SELECT b.mv_id,b.track_id,a.attr_tp,b.mvtrack_id,ROW_NUMBER () OVER (PARTITION BY b.mv_id ORDER BY b.mvtrack_id ) as mv_rank
                        FROM ted_mv@cfeel a, ted_mvtrack@cfeel b
                        WHERE a.mv_id=b.mv_id and b.db_sts ='A')
                WHERE mv_rank=1)bb,
                track cc, ted_mv_w@cfeel dd
            WHERE aa.mv_id = bb.mv_id AND bb.track_id = cc.track_id
            AND aa.mv_id = dd.mv_id and dd.nation_cd = 'JPN'
            AND (
                EXISTS (
                    SELECT 1 FROM ted_mvtrack@cfeel ee
                    WHERE dd.mv_id = ee.mv_id
                    AND ee.upd_dt > trunc(SYSDATE + m_time_offset)
                )
                OR (dd.crt_dt > trunc(SYSDATE + m_time_offset) OR dd.upd_dt > trunc(SYSDATE + m_time_offset))
                OR (aa.crt_dt > trunc(SYSDATE + m_time_offset) OR aa.upd_dt > trunc(SYSDATE + m_time_offset))
            )
        ) b
        ON (a.mv_id = b.mv_id)
        WHEN MATCHED THEN
            UPDATE SET track_id = b.track_id, artist_id = b.artist_id, media_no = b.media_no, mv_title = b.mv_title, nation_cd = b.nation_cd,
                attr_tp = b.attr_tp, highrate_yn = b.high_yn, actor = b.actor, dscr = b.dscr, release_ymd = b.release_ymd,
                media_yn = b.media_yn, crt_dt = b.crt_dt, upd_dt = sysdate, status = b.status,
                svc_fullhd_yn = b.svc_fullhd_yn, svc_hd_yn = b.svc_hd_yn, svc_sd_yn = b.svc_sd_yn, svc_mp4_yn = b.svc_mp4_yn
        WHEN NOT MATCHED THEN
            INSERT (mv_id,track_id,artist_id,media_no,mv_title,nation_cd,attr_tp,highrate_yn,actor,dscr,release_ymd,
                media_yn,crt_dt,upd_dt,status,svc_fullhd_yn,svc_hd_yn,svc_sd_yn,svc_mp4_yn)
            VALUES (b.mv_id, b.track_id, b.artist_id, b.media_no, b.mv_title, b.nation_cd, b.attr_tp, b.high_yn, b.actor, b.dscr, b.release_ymd,
                b.media_yn, b.crt_dt, b.upd_dt, b.status, b.svc_fullhd_yn, b.svc_hd_yn, b.svc_sd_yn, b.svc_mp4_yn);
        
        --dbms_output.put_line(rec_mv.mv_id);
        
        MERGE INTO mv_local a
        USING (
            SELECT aa.mv_id, nation_cd as local_cd, mv_title, crt_dt, decode(db_sts, 'A', 'OK', 'BLIND') as status
            FROM ted_mv_w@cfeel aa
            WHERE ((aa.nation_cd = 'JPN' AND (aa.crt_dt > trunc(SYSDATE + m_time_offset) OR aa.upd_dt > trunc(SYSDATE + m_time_offset)))
             
            )AND EXISTS(SELECT 1 FROM mv bb WHERE aa.mv_id = bb.mv_id)
        ) b
        ON (a.mv_id = b.mv_id AND a.local_cd = 'JPN')
        WHEN MATCHED THEN
            UPDATE SET mv_title_local = b.mv_title, upd_dt = sysdate, status = b.status
        WHEN NOT MATCHED THEN
            INSERT (mv_id, local_cd, mv_title_local, crt_dt, status)
            VALUES (b.mv_id, 'JPN', b.mv_title, b.crt_dt, b.status);
    END;
    
    BEGIN
        UPDATE album_local a
        SET status = 'BLIND', upd_dt = sysdate
        WHERE NOT EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 716 AND right_yn = 'Y') OR (service_id = 717 AND right_yn = 'Y') OR (service_id = 718 AND right_yn = 'Y')))
        AND local_cd = 'JPN'; 
            
        UPDATE album_local a
        SET status = 'OK', upd_dt = sysdate
        WHERE EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 716 AND right_yn = 'Y') OR (service_id = 717 AND right_yn = 'Y') OR (service_id = 718 AND right_yn = 'Y')))
        AND status != 'OK'
        AND local_cd = 'JPN'; 
    END;

END jpn_meta_sync;

PROCEDURE jpn_rights_sync(p_time_offset NUMBER)
IS
    /*
        716	일본스트리밍
        717	일본다운로드
        718	일본임대제
    */
    m_str_svc_id NUMBER := 716; 
    m_dnl_svc_id NUMBER := 717;
    m_rent_svc_id NUMBER := 718; 
    m_target_dt DATE := trunc(sysdate -1);
BEGIN

    IF p_time_offset IS NOT NULL THEN 
        m_target_dt := trunc(sysdate + p_time_offset);
    ELSE
        m_target_dt := trunc(sysdate -1);
    END IF;
    
    -- STR RIGHTS
    UPDATE track_local a
    SET (str_rights_yn, mv_str_rights_yn) = 
        (SELECT nvl(right_yn, 'N'), nvl(right_yn, 'N') FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_str_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)), upd_dt = sysdate
    WHERE 
        local_cd = 'JPN' AND
        EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_str_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)
        );
    
    -- DNL RIGHTS
    UPDATE track_local a
    SET (dnl_rights_yn) = 
        (SELECT nvl(right_yn, 'N') FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_dnl_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)), upd_dt = sysdate
    WHERE 
        local_cd = 'JPN' AND
        EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_dnl_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)
        );
        
    -- RENT RIGHTS
    UPDATE track_local a
    SET (rent_rights_yn) = 
        (SELECT nvl(right_yn, 'N') FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_rent_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)), upd_dt = sysdate
    WHERE 
        local_cd = 'JPN' AND
        EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b WHERE a.track_id = b.track_id AND b.service_id = m_rent_svc_id 
            AND (crt_dt > m_target_dt OR upd_dt > m_target_dt)
        );
        
  -----2010.10.10. dbmaster added---  
    BEGIN
        UPDATE album_local a
        SET status = 'BLIND', upd_dt = sysdate
        WHERE NOT EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 716 AND right_yn = 'Y') OR (service_id = 717 AND right_yn = 'Y') OR (service_id = 718 AND right_yn = 'Y')))
        AND local_cd = 'JPN'; 
            
        UPDATE album_local a
        SET status = 'OK', upd_dt = sysdate
        WHERE EXISTS(SELECT 1 FROM ttmp_trackright@cfeel b 
            WHERE a.album_id = b.album_id AND ((service_id = 716 AND right_yn = 'Y') OR (service_id = 717 AND right_yn = 'Y') OR (service_id = 718 AND right_yn = 'Y')))
        AND status != 'OK'
        AND local_cd = 'JPN'; 
    END;


END jpn_rights_sync;

END global_job;
