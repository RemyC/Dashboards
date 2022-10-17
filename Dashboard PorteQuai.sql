SELECT 
dd.DOCK_DOOR_NAME                                                   as Porte_de_quai
,shpt.TC_SHIPMENT_ID                                                as chargement
,substr(shpt.tc_shipment_id, 1, length(shpt.tc_shipment_id)-5)      as chgt_left
,substr(shpt.tc_shipment_id, -5 )                                   as chgt_right
,TO_CHAR(shpt.pickup_start_dttm, 'DD/MM/YY HH24:Mi')                as pickup
,TO_CHAR(shpt.pickup_start_dttm, 'DD/MM')                           as pickup_date
,TO_CHAR(shpt.pickup_start_dttm, 'HH24:Mi')                         as pickup_hour
,stop.STOP_SEQ                                                      as Arret
,stop.facility_alias_id                                             as Ctmq
,fa.city                                                            as Ville
,shpt.SIZE1_VALUE                                                   as Metrage
,shpt.ASSIGNED_CARRIER_CODE                                         as Transporteur    
,lpn_advancing_meca.nb_prep                                         as nb_prep_meca
,lpn_advancing_meca.nb_ancr                                         as nb_ancr_meca
,lpn_advancing_meca.nb_chgt                                         as nb_chgt_meca
,lpn_advancing_tradi.nb_prep                                        as nb_prep_tradi
,lpn_advancing_tradi.nb_ancr                                        as nb_ancr_tradi
,lpn_advancing_tradi.nb_chgt                                        as nb_chgt_tradi
,trunc(SUM(lpn_global.chgt)/ SUM(lpn_global.total)*100)             as percentage
,shpt_weight.weight                                                 as poids_charge
,shpn_order.ordering                                                as note_ordre
,shpn_add.adding                                                    as note_rajout

FROM SHIPMENT shpt
INNER JOIN 
    (select ddr.shipment_id,dd.DOCK_DOOR_NAME
    from dock_door dd
    inner join dock_door_ref ddr on ddr.DOCK_DOOR_ID=dd.DOCK_DOOR_ID
    )dd on dd.SHIPMENT_ID=shpt.SHIPMENT_ID
INNER JOIN STOP on stop.SHIPMENT_ID=shpt.SHIPMENT_ID
INNER JOIN facility_alias fa on fa.facility_alias_id=stop.facility_alias_id
LEFT JOIN
    (
    SELECT
        shipment_id
        ,d_facility_alias_id
        , SUM(lpn_advancement.prep) as nb_prep
        , SUM(lpn_advancement.ancr) as nb_ancr
        , SUM(lpn_advancement.chgt) as nb_chgt
	FROM
		(
		SELECT 
			lpnh.shipment_id
            ,lpnh.d_facility_alias_id
			, case 
				when 
					(
					lpnh.lpn_facility_status < 20
					OR
					( lpnh.lpn_facility_status BETWEEN 20 AND 30 AND ( locnh.locn_class = 'R' OR locnh.locn_class IS NULL ) )
					) 
				then 1 
				else 0 
			end as prep
			, case 
				when
					(
					(lpnh.lpn_facility_status BETWEEN 20 AND 30 AND locnh.locn_class = 'S')
					OR lpnh.lpn_facility_status BETWEEN 31 AND 49
					)
				then 1 
				else 0 
			end as ancr
			, case when lpnh.lpn_facility_status = 50 then 1 else 0 end as chgt
	   FROM lpn lpnh
	   LEFT JOIN locn_hdr locnh ON lpnh.curr_sub_locn_id = locnh.locn_id
	   WHERE 1=1
	   AND lpnh.shipment_id is not null
       AND container_type IN ('03','05','06','PTS')
		) lpn_advancement
	WHERE 1=1
	GROUP BY shipment_id, d_facility_alias_id
    ) lpn_advancing_meca on shpt.shipment_id = lpn_advancing_meca.shipment_id 
        and stop.FACILITY_ALIAS_ID =lpn_advancing_meca.d_facility_alias_id


LEFT JOIN
    (
    SELECT
        shipment_id
        ,d_facility_alias_id
        , SUM(lpn_advancement.prep) as nb_prep
        , SUM(lpn_advancement.ancr) as nb_ancr
        , SUM(lpn_advancement.chgt) as nb_chgt
	FROM
		(
		SELECT 
			lpnh.shipment_id
            ,lpnh.d_facility_alias_id
			, case 
				when 
					(
					lpnh.lpn_facility_status < 20
					OR
					( lpnh.lpn_facility_status BETWEEN 20 AND 30 AND ( locnh.locn_class = 'R' OR locnh.locn_class IS NULL ) )
					) 
				then 1 
				else 0 
			end as prep
			, case 
				when
					(
					(lpnh.lpn_facility_status BETWEEN 20 AND 30 AND locnh.locn_class = 'S')
					OR lpnh.lpn_facility_status BETWEEN 31 AND 49
					)
				then 1 
				else 0 
			end as ancr
			, case when lpnh.lpn_facility_status = 50 then 1 else 0 end as chgt
	   FROM lpn lpnh
	   LEFT JOIN locn_hdr locnh ON lpnh.curr_sub_locn_id = locnh.locn_id
	   WHERE 1=1
	   AND lpnh.shipment_id is not null
       AND (NOT lpnh.container_type IN ('03','05','06','PTS')
            OR
            lpnh.container_type IS NULL)
		) lpn_advancement
	WHERE 1=1
	GROUP BY shipment_id, d_facility_alias_id
    ) lpn_advancing_tradi on shpt.shipment_id = lpn_advancing_tradi.shipment_id 
        and stop.FACILITY_ALIAS_ID =lpn_advancing_tradi.d_facility_alias_id

LEFT JOIN
    (SELECT
		lpnh.shipment_id
		, case when lpnh.lpn_facility_status = 50 then 1 else 0 end as chgt
		, case when lpnh.lpn_facility_status <= 99 then 1 else 0 end as total
    FROM lpn lpnh
    WHERE lpnh.shipment_id is not null
    ) lpn_global on shpt.shipment_id = lpn_global.shipment_id
LEFT JOIN 
    (SELECT tc_shipment_id, SUM(estimated_weight) as weight
    FROM lpn 
    WHERE lpn_facility_status = 50
    AND  tc_shipment_id IS NOT NULL
    GROUP BY tc_shipment_id
    ) shpt_weight on shpt_weight.tc_shipment_id=shpt.tc_shipment_id
LEFT JOIN
    (select shipment_id, substr(note,2,499) as ordering from shipment_note where substr(note,1,1)='@'
    ) shpn_order on shpn_order.shipment_id=shpt.shipment_id
LEFT JOIN
    (select shipment_id, substr(note,2,499) as adding from shipment_note where substr(note,1,1)='+'
    ) shpn_add on shpn_add.shipment_id=shpt.shipment_id    

WHERE 1=1
and shpt.SHIPMENT_STATUS < 80 
and stop.STOP_SEQ > 1

group by shpt.tc_shipment_id
, dd.DOCK_DOOR_NAME
, substr(shpt.tc_shipment_id, 1, length(shpt.tc_shipment_id)-5)
, substr(shpt.tc_shipment_id, -5 )
, TO_CHAR(shpt.pickup_start_dttm, 'DD/MM/YY HH24:Mi')
, TO_CHAR(shpt.pickup_start_dttm, 'DD/MM')
, TO_CHAR(shpt.pickup_start_dttm, 'HH24:Mi')
, stop.STOP_SEQ
, stop.facility_alias_id
, fa.city
, shpt.SIZE1_VALUE
, shpt.ASSIGNED_CARRIER_CODE
,lpn_advancing_meca.nb_prep
,lpn_advancing_meca.nb_ancr
,lpn_advancing_meca.nb_chgt
,lpn_advancing_tradi.nb_prep
,lpn_advancing_tradi.nb_ancr
,lpn_advancing_tradi.nb_chgt
, shpt_weight.weight
, shpn_order.ordering
, shpn_add.adding

ORDER BY dd.DOCK_DOOR_NAME ASC, stop.STOP_SEQ DESC
;