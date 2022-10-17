"SELECT
	shipth.tc_shipment_id															as tc_shipment_id
	, substr(shipth.tc_shipment_id, 1, length(shipth.tc_shipment_id)-5)				as tc_shipment_id_left
	, substr(shipth.tc_shipment_id, -5 )											as tc_shipment_id_right
    , COALESCE(dsg_carrier_code,shipth.assigned_carrier_code)						as carrier_code
    , shipth.broker_ref																as store_code
    , shipn_debord.note																as debord
    , TO_CHAR(shipth.pickup_start_dttm, 'DD/MM/YY HH24:Mi')							as pickup_start
    , TO_CHAR(shipth.pickup_start_dttm, 'DD/MM')									as pickup_start_date
    , TO_CHAR(shipth.pickup_start_dttm, 'HH24:Mi')	    							as pickup_start_hour
    , SUBSTR(locnh.aisle,-1,1) || locnh.lvl											as location
    , shipn_user.note																as users
    , dock_door.dock_door															as dock_door
    , TO_CHAR(shipt_trkg.start_of_loading, 'DD/MM HH24:Mi')							as start_of_loading
    , TO_CHAR(shipt_trkg.start_of_loading, 'DD/MM')									as start_of_loading_date
    , TO_CHAR(shipt_trkg.start_of_loading, 'HH24:Mi')	    						as start_of_loading_hour
    , shipn_coch.note																as coch
    , lpn_advancing.loading_advancement												as global_advancement
    , lpn_advancing.prep															as global_prep
    , lpn_advancing.ancr															as global_ancr
    , lpn_advancing.chgt															as global_chgt
    , lpn_advancing_meca.loading_advancement										as meca_advancement
    , lpn_advancing_meca.prep														as meca_prep
    , lpn_advancing_meca.ancr														as meca_ancr
    , lpn_advancing_meca.chgt														as meca_chgt
    , lpn_advancing_tradi.loading_advancement										as tradi_advancement
    , lpn_advancing_tradi.prep														as tradi_prep
    , lpn_advancing_tradi.ancr														as tradi_ancr
    , lpn_advancing_tradi.chgt														as tradi_chgt
	, lpn_advancing.nb_prep															as global_nb_prep
	, lpn_advancing.nb_ancr                                             			as global_nb_ancr
	, lpn_advancing.nb_chgt                                             			as global_nb_chgt
	, lpn_advancing_meca.nb_prep                                        			as meca_nb_prep
	, lpn_advancing_meca.nb_ancr                                        			as meca_nb_ancr
	, lpn_advancing_meca.nb_chgt                                        			as meca_nb_chgt
	, lpn_advancing_tradi.nb_prep                                       			as tradi_nb_prep
	, lpn_advancing_tradi.nb_ancr                                       			as tradi_nb_ancr
	, lpn_advancing_tradi.nb_chgt                                       			as tradi_nb_chgt
	, shipment_not_closed.tc_shipment_id											as not_closed
    /*20210702: Rajout ship_opti + ref_field*/
    , upper(substr(ship_opti.min_first_name,1,1))||lower(substr(ship_opti.min_first_name,2,50))         as opti_nom_debut
    , to_char(ship_opti.min_created_dttm, 'DD/MM HH24:Mi')                                              as opti_date_debut
    , upper(substr(ship_opti.max_first_name,1,1))||lower(substr(ship_opti.max_first_name,2,50))         as opti_nom_fin
    , to_char(ship_opti.max_created_dttm, 'DD/MM HH24:Mi')                                              as opti_date_fin
    , shipt_trkg.last_loader                                                                            as ref_field1
    , 0                                                                                                 as ref_field2
    , 0                                                                                                 as ref_field3

FROM shipment shipth
--20210826 : Si Une DO est sur plusieurs chargements, alors o.shipment_id est null >> Jointure avec table LPN
--INNER JOIN (SELECT DISTINCT shipment_id FROM orders WHERE do_type='10') ordh on shipth.shipment_id = ordh.shipment_id
INNER JOIN (
    SELECT distinct l.shipment_id
    FROM orders o
    LEFT JOIN lpn l on l.order_id=o.order_id 
    WHERE 1=1 
    AND o.do_type='10'
    AND l.shipment_id IS NOT NULL
) ordh on shipth.shipment_id = ordh.shipment_id
LEFT JOIN locn_hdr locnh ON shipth.staging_locn_id = locnh.locn_id
LEFT JOIN (
    select shipment_id, SUBSTR(dd.dock_door_name,1,1) || SUBSTR(dd.dock_door_name,-2,2) as dock_door
    from dock_door_ref ddf
    inner join dock_door dd on ddf.dock_door_id=dd.dock_door_id
    ) dock_door ON shipth.shipment_id = dock_door.shipment_id
--20210825 Rajout début et fin chargement
--20210826 Rajout distinct + Group by poureviter doublons en cas de plusieurs chargeurs
/*LEFT JOIN
    (
    SELECT  ref_field_1 as tc_shipment_id
        , MIN(trkg.create_date_time)       as start_of_loading
    FROM    prod_trkg_tran trkg
    WHERE   1=1
    AND     trkg.menu_optn_name='RF CHARGER OLPN'
    GROUP BY ref_field_1
    )
    shipt_trkg ON shipth.tc_shipment_id = shipt_trkg.tc_shipment_id*/
LEFT JOIN(
    SELECT	
		min_load.tc_shipment_id
		--min_load.tc_shipment_id
		, min_load.min_created_dttm as start_of_loading
		--, min_load.first_user as first_loader
		--, max_load.max_created_dttm as end_of_loading
		, upper(substr(max_load.last_user,1,1))||lower(substr(max_load.last_user,2,50)) as last_loader
    FROM
        (
		SELECT	trkg.ref_field_1 as tc_shipment_id, min_load.min_created_dttm, uu.user_first_name as last_user, MIN(trkg.seq_nbr) as begin
        FROM prod_trkg_tran trkg
        INNER JOIN ucl_user uu ON uu.user_name = trkg.user_id
        LEFT JOIN
            (
            SELECT  trkg.ref_field_1 as tc_shipment_id, MIN(trkg.create_date_time) as min_created_dttm
            FROM prod_trkg_tran trkg
            WHERE   1=1
            AND     trkg.menu_optn_name='RF CHARGER OLPN'
            GROUP BY trkg.ref_field_1
            )
            min_load ON trkg.ref_field_1 = min_load.tc_shipment_id AND trkg.create_date_time = min_load.min_created_dttm
        WHERE	1=1
        AND     trkg.menu_optn_name='RF CHARGER OLPN'
        AND 	min_load.min_created_dttm IS NOT NULL
        GROUP BY trkg.ref_field_1, min_load.min_created_dttm, uu.user_first_name
        )
        min_load
    LEFT JOIN
        (
        SELECT	trkg.ref_field_1 as tc_shipment_id, max_load.max_created_dttm, uu.user_first_name as last_user, MAX(trkg.seq_nbr) as last
        FROM prod_trkg_tran trkg
        INNER JOIN ucl_user uu ON uu.user_name = trkg.user_id
        LEFT JOIN
            (
            SELECT  trkg.ref_field_1 as tc_shipment_id, MAX(trkg.create_date_time) as max_created_dttm
            FROM prod_trkg_tran trkg
            WHERE   1=1
            AND     trkg.menu_optn_name='RF CHARGER OLPN'
            GROUP BY trkg.ref_field_1
            )
            max_load ON trkg.ref_field_1 = max_load.tc_shipment_id AND trkg.create_date_time = max_load.max_created_dttm
        WHERE	1=1
        AND     trkg.menu_optn_name='RF CHARGER OLPN'
        AND 	max_load.max_created_dttm IS NOT NULL
        GROUP BY trkg.ref_field_1, max_load.max_created_dttm, uu.user_first_name
        )
        max_load on min_load.tc_shipment_id = max_load.tc_shipment_id
    WHERE	1=1
    group by (min_load.tc_shipment_id), min_load.min_created_dttm, upper(substr(max_load.last_user,1,1))||lower(substr(max_load.last_user,2,50))
    ) shipt_trkg ON shipth.tc_shipment_id = shipt_trkg.tc_shipment_id

LEFT JOIN
    (
	SELECT
			shipment_id
			, ROUND(((SUM(lpn_advancement.prep)*100)/COUNT(*)),0) as prep
			, ROUND(((SUM(lpn_advancement.ancr)*100)/COUNT(*)),0) as ancr
			, ROUND(((SUM(lpn_advancement.chgt)*100)/COUNT(*)),0) as chgt
			, LPAD(ROUND(((SUM(lpn_advancement.prep)*100)/COUNT(*)),0),3,0) || ' / ' || LPAD(ROUND(((SUM(lpn_advancement.ancr)*100)/COUNT(*)),0),3,0) || ' / '  || LPAD(ROUND(((SUM(lpn_advancement.chgt)*100)/COUNT(*)),0),3,0) as loading_advancement
			, SUM(lpn_advancement.prep) as nb_prep
			, SUM(lpn_advancement.ancr) as nb_ancr
			, SUM(lpn_advancement.chgt) as nb_chgt
	FROM
			(
		   SELECT lpnh.shipment_id
				   /* 20190924 : CORRECTION CHIFFRAGE PREP
				   --, case when lpnh.lpn_facility_status < 20 then 1 else 0 end as prep
				   --, case when lpn_facility_status BETWEEN 20 AND 49 then 1 else 0 end as ancr
				   */
				   , case when
					   (
							   lpnh.lpn_facility_status < 20
							   OR
							   ( lpnh.lpn_facility_status BETWEEN 20 AND 30 AND ( locnh.locn_class in ('R','P') OR locnh.locn_class IS NULL ) )
					   )
				   then 1 else 0 end as prep
				   , case when
					   (
							   (lpnh.lpn_facility_status BETWEEN 20 AND 30 AND locnh.locn_class = 'S')
							   OR
							   lpnh.lpn_facility_status BETWEEN 31 AND 49
					   )
					   then 1 else 0 end as ancr
				   , case when lpnh.lpn_facility_status >= 50 then 1 else 0 end as chgt
		   FROM lpn lpnh
		   LEFT JOIN locn_hdr locnh ON lpnh.curr_sub_locn_id = locnh.locn_id
		   WHERE 1=1
		   AND          NOT lpnh.shipment_id is null
			)
	lpn_advancement
	WHERE 1=1
	GROUP BY shipment_id
    )
    lpn_advancing on shipth.shipment_id = lpn_advancing.shipment_id
LEFT JOIN
    (
	SELECT
		shipment_id
		, ROUND(((SUM(lpn_advancement.prep)*100)/COUNT(*)),0) as prep
		, ROUND(((SUM(lpn_advancement.ancr)*100)/COUNT(*)),0) as ancr
		, ROUND(((SUM(lpn_advancement.chgt)*100)/COUNT(*)),0) as chgt
		, LPAD(ROUND(((SUM(lpn_advancement.prep)*100)/COUNT(*)),0),3,0) || ' / ' || LPAD(ROUND(((SUM(lpn_advancement.ancr)*100)/COUNT(*)),0),3,0) || ' / '  || LPAD(ROUND(((SUM(lpn_advancement.chgt)*100)/COUNT(*)),0),3,0) as loading_advancement
		, SUM(lpn_advancement.prep) as nb_prep
		, SUM(lpn_advancement.ancr) as nb_ancr
		, SUM(lpn_advancement.chgt) as nb_chgt
	FROM
		(
	   SELECT lpnh.shipment_id
		   /* 20190924 : CORRECTION CHIFFRAGE PREP
		   --, case when lpnh.lpn_facility_status < 20 then 1 else 0 end as prep
		   --, case when lpn_facility_status BETWEEN 20 AND 49 then 1 else 0 end as ancr
		   */
		   , case when
			   (
					   lpnh.lpn_facility_status < 20
					   OR
					   ( lpnh.lpn_facility_status BETWEEN 20 AND 30 AND ( locnh.locn_class in ('R','P') OR locnh.locn_class IS NULL ) )
			   )
		   then 1 else 0 end as prep
		   , case when
			   (
					   (lpnh.lpn_facility_status BETWEEN 20 AND 30 AND locnh.locn_class = 'S')
					   OR
					   lpnh.lpn_facility_status BETWEEN 31 AND 49
			   )
			   then 1 else 0 end as ancr
		   , case when lpnh.lpn_facility_status >= 50 then 1 else 0 end as chgt
	   FROM lpn lpnh
	   LEFT JOIN locn_hdr locnh ON lpnh.curr_sub_locn_id = locnh.locn_id
	   WHERE 1=1
	   AND          NOT lpnh.shipment_id is null
	   AND          container_type IN ('03','05','06','PTS')
		)
	lpn_advancement
	WHERE 1=1
	GROUP BY shipment_id
    )
    lpn_advancing_meca on shipth.shipment_id = lpn_advancing_meca.shipment_id
LEFT JOIN
    (
	SELECT shipment_id
		, ROUND(((SUM(lpn_advancement.prep)*100)/COUNT(*)),0) as prep
		, ROUND(((SUM(lpn_advancement.ancr)*100)/COUNT(*)),0) as ancr
		, ROUND(((SUM(lpn_advancement.chgt)*100)/COUNT(*)),0) as chgt
		, LPAD(ROUND(((SUM(lpn_advancement.prep)*100)/COUNT(*)),0),3,0) || ' / ' || LPAD(ROUND(((SUM(lpn_advancement.ancr)*100)/COUNT(*)),0),3,0) || ' / '  || LPAD(ROUND(((SUM(lpn_advancement.chgt)*100)/COUNT(*)),0),3,0) as loading_advancement
		, SUM(lpn_advancement.prep) as nb_prep
		, SUM(lpn_advancement.ancr) as nb_ancr
		, SUM(lpn_advancement.chgt) as nb_chgt
	FROM
		(
	   SELECT lpnh.shipment_id
		   /* 20190924 : CORRECTION CHIFFRAGE PREP
		   --, case when lpnh.lpn_facility_status < 20 then 1 else 0 end as prep
		   --, case when lpn_facility_status BETWEEN 20 AND 49 then 1 else 0 end as ancr
		   */
		   , case when
			   (
				   lpnh.lpn_facility_status < 20
				   OR
				   ( lpnh.lpn_facility_status BETWEEN 20 AND 30 AND ( locnh.locn_class in ('R','P') OR locnh.locn_class IS NULL ) )
			   )
		   then 1 else 0 end as prep
		   , case when
			   (
				   (lpnh.lpn_facility_status BETWEEN 20 AND 30 AND locnh.locn_class = 'S')
				   OR
				   lpnh.lpn_facility_status BETWEEN 31 AND 49
			   )
			   then 1 else 0 end as ancr
		   , case when lpnh.lpn_facility_status >= 50 then 1 else 0 end as chgt
	   FROM lpn lpnh
	   LEFT JOIN locn_hdr locnh ON lpnh.curr_sub_locn_id = locnh.locn_id
	   WHERE 1=1
	   AND          NOT lpnh.shipment_id is null
	   AND          ( NOT lpnh.container_type IN ('03','05','06','PTS')
					   OR
						lpnh.container_type IS NULL
					)
		)
	lpn_advancement
	WHERE 1=1
	GROUP BY shipment_id
    )
    lpn_advancing_tradi on shipth.shipment_id = lpn_advancing_tradi.shipment_id
LEFT JOIN (select shipment_id, substr(note,2,499) as note from shipment_note where substr(note,1,1)='#') shipn_debord ON shipth.shipment_id = shipn_debord.shipment_id
LEFT JOIN (select shipment_id, substr(note,2,499) as note from shipment_note where substr(note,1,1)='$') shipn_user ON shipth.shipment_id = shipn_user.shipment_id
LEFT JOIN (select shipment_id, substr(note,2,499) as note from shipment_note where substr(note,1,1)='@') shipn_coch ON shipth.shipment_id = shipn_coch.shipment_id
LEFT JOIN
        (
		SELECT      LISTAGG(shipth.tc_shipment_id, ' ') WITHIN GROUP (ORDER BY shipth.tc_shipment_id) as tc_shipment_id
		FROM        shipment shipth
		WHERE      1=1
        AND shipth.shipment_status BETWEEN 40 AND 79
		AND shipth.pickup_start_dttm BETWEEN TRUNC(sysdate-10) AND TRUNC(sysdate-5)
        AND EXISTS( SELECT shipment_id FROM lpn WHERE shipth.shipment_id = lpn.shipment_id)
        )
        shipment_not_closed ON 1=1
/*20210702 : code blocage, début optimisation*/
LEFT JOIN (
        SELECT shipment_id, min(min_created_dttm) as min_created_dttm, min(min_first_name) as min_first_name, min(max_created_dttm) as max_created_dttm, min(max_first_name) as max_first_name
        FROM
        (
            SELECT  l.shipment_id, min_lock.min_created_dttm, max_lock.max_created_dttm
                    , CASE WHEN ll.created_dttm = min_lock.min_created_dttm THEN uu.user_first_name ELSE NULL END as min_first_name
                    , CASE WHEN ll.created_dttm = max_lock.max_created_dttm THEN uu.user_first_name ELSE NULL END as max_first_name
            FROM lpn l
            INNER JOIN lpn_lock ll on ll.lpn_id=l.lpn_id
            INNER JOIN ucl_user uu ON uu.user_name=ll.created_source
            LEFT JOIN
                (
                SELECT  l.shipment_id, min(ll.created_dttm) as min_created_dttm --, max(ll.created_dttm) as max_created_dttm
                FROM lpn l
                INNER JOIN lpn_lock ll on ll.lpn_id=l.lpn_id
                where   1=1
                AND ll.inventory_lock_code ='OP' 
                GROUP BY l.shipment_id
                )
                min_lock ON l.shipment_id = min_lock.shipment_id AND ll.created_dttm = min_lock.min_created_dttm
            LEFT JOIN
                (
                SELECT  l.shipment_id, max(ll.created_dttm) as max_created_dttm --, max(ll.created_dttm) as max_created_dttm
                FROM lpn l
                INNER JOIN lpn_lock ll on ll.lpn_id=l.lpn_id
                where   1=1
                AND ll.inventory_lock_code ='OP' 
                GROUP BY l.shipment_id
                )
                max_lock ON l.shipment_id = max_lock.shipment_id AND ll.created_dttm = max_lock.max_created_dttm
            WHERE   1=1
            --AND l.shipment_id='327847'
            AND ll.inventory_lock_code ='OP' 
            AND ( 
                    CASE
                        WHEN NOT min_lock.shipment_id IS NULL THEN 1
                        WHEN NOT max_lock.shipment_id IS NULL THEN 1
                        ELSE 0
                    END
                ) = 1
        )
        WHERE   1=1

        GROUP BY shipment_id
        )
        ship_opti on ship_opti.shipment_id=shipth.shipment_id

WHERE 1=1
AND shipth.shipment_status BETWEEN 40 AND 79
AND TRUNC(shipth.pickup_start_dttm) > TRUNC(sysdate-4)
/* 20191224 : Pour eviter soucis de jour fériee
AND TRUNC(shipth.pickup_start_dttm) BETWEEN TRUNC(sysdate-4) AND 
        (
        CASE
                -- /!\ CASE PARAMETRES REGIONNAUX : POUR LE VENDREDI , affichage du 6 au lieu de 5 à cause de SCI  /!\ 
                WHEN to_char(sysdate,'D') = 6
                        THEN TRUNC(sysdate+3)
                ELSE TRUNC(sysdate+1)
        END
        )
*/
--AND ( locnh.dsp_locn <> 'BTB-ANC-NAV' OR locnh.dsp_locn IS NULL )
AND NOT lpn_advancing.loading_advancement IS NULL"