SELECT
            TO_CHAR(appth.scheduled_dttm, 'DD/MM')						                                                            as scheduled_date
,           TO_CHAR(appth.scheduled_dttm, 'HH24:Mi')					                                                            as scheduled_time
--,           appth.appointment_id										                                                              as appointment_id
,           appth.tc_appointment_id										                                                            as tc_appointment_id
,           appth.comments                                                                                                          as appointment_comments
,           appths.description                                                                                                      as appointment_status
,           apptcnt.attribute_value                                                                                                 as tc_container_id
,           COALESCE(CAST(apptpal.nb_pal  AS DECIMAL(10,3) ),0)                                                                     as nb_palette
,           COALESCE(CAST(apptpack.nb_package  AS DECIMAL(10,3) ),0)                                                                as nb_package
,           apptarriv.date_arrival                                                                                                  as date_arrival
,           apptsend.attribute_value                                                                                                as send_to_dock
,           slots.description                                                                                                       as dock_group
,           CASE
                WHEN slots.description = 'BATIMENT A'                           THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT A (EXTERIEUR)'               THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT A (INTERIEUR)'               THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT A (CONTAINER)'               THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT B'                           THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT B (CAMION)'                  THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT B (CONTAINER)'               THEN 'POLE_AB'
                WHEN slots.description = 'BATIMENT C'                           THEN 'POLE_C'
                WHEN slots.description = 'BATIMENT C (CAMION)'                  THEN 'POLE_C'
                WHEN slots.description = 'BATIMENT C (CONTAINER)'               THEN 'POLE_C'
                WHEN slots.description = 'BATIMENT C1'                          THEN 'POLE_C'
                WHEN slots.description = 'RDV DECOCLICO'                        THEN 'POLE_C'
                WHEN slots.description = 'RDV LOL'                              THEN 'POLE_AB'
                WHEN slots.description = 'RDV RETOURS CONTENANT/MAGS'           THEN 'POLE_AB'
                ELSE 'TO_DEFINE'
            END                                                                                                                     as dock_pole
,           buss.description																									    as business_partner
,           pordhd.purchase_orders_running																						    as purchase_orders_running
,           pordhd.purchase_orders_closed                                                                                           as purchase_orders_closed
,           pordhd.purchase_orders_priority                                                                                         as purchase_orders_priority
,           pordhd.nb_lines                                                                                                         as purchase_order_nb_lines
,           trailerhd.checkin_dttm                                                                                                  as checkin_dttm
,           (EXTRACT(HOUR FROM (SYSDATE - trailerhd.checkin_dttm))*60) + EXTRACT(MINUTE FROM (SYSDATE - trailerhd.checkin_dttm))    as checkin_dttm_gap
,           trailerhd.checkout_dttm                                                                                                 as checkout_dttm
,           trailerhd.type                                                                                                          as trailer_type
,           trailerhd.trailer_id																								    as seal_number
,           trailerhd.min_task																									    as start_dttm
,           ROUND((SYSDATE - trailerhd.min_task)*24*60) 																            as start_dttm_gap
,           trailerhd.max_task																									    as end_dttm
,           trailerhd.dsp_locn																									    as dsp_locn
,           apptcomm.attribute_value                                                                                                as ref_field1
,           0                                                                                                                       as ref_field2
,           0                                                                                                                       as ref_field3

FROM        ilm_appointments appth
LEFT JOIN   ilm_appointment_status appths ON appth.appt_status = appths.appt_status_code

LEFT JOIN   (
                SELECT      appointment_id																							as appointment_id
                ,           appt_obj_type																							as appt_obj_type
                ,           LISTAGG(purchase_orders_running, ' ') WITHIN GROUP (ORDER BY purchase_orders_running)					as purchase_orders_running
                ,           LISTAGG(purchase_orders_closed, ' ') WITHIN GROUP (ORDER BY purchase_orders_closed)						as purchase_orders_closed
                ,           MAX(business_partner_id)																				as business_partner_id
                ,           MIN(purchase_orders_priority)																			as purchase_orders_priority
                ,           SUM(nb_lines)																							as nb_lines

                FROM (
                        SELECT      apptobj.appointment_id                                                                          as appointment_id
                        ,           apptobj.appt_obj_type                                                                           as appt_obj_type
                        ,           CASE 
                                        WHEN pordh.purchase_orders_status <> 950 THEN pordh.tc_purchase_orders_id 
                                        ELSE NULL 
                                    END                                                                                             as purchase_orders_running
                        ,           CASE 
                                        WHEN pordh.purchase_orders_status = 950 THEN pordh.tc_purchase_orders_id 
                                        ELSE NULL 
                                    END                                                                                             as purchase_orders_closed
                        ,           pordh.business_partner_id                                                                       as business_partner_id
                        ,           CASE
                                        WHEN SUBSTR(pordn.note,1,2) = 'PU' THEN 1
                                        WHEN SUBSTR(pordn.note,1,2) = 'CU' THEN 2
                                        WHEN SUBSTR(pordn.note,1,2) = 'WU' THEN 3
                                        WHEN SUBSTR(pordn.note,1,2) = 'P0' THEN 4
                                        WHEN SUBSTR(pordn.note,1,2) = 'C0' THEN 5
                                        WHEN SUBSTR(pordn.note,1,2) = 'W0' THEN 6
                                        WHEN SUBSTR(pordn.note,1,2) = 'UR' THEN 7
                                        ELSE 8
                                    END                                                                                             as purchase_orders_priority
                        ,           COUNT(*)                                                                                        as nb_lines

                        FROM        ilm_appointment_objects apptobj
                        LEFT JOIN   purchase_orders pordh   ON apptobj.appt_obj_id = pordh.purchase_orders_id
                        LEFT JOIN 	purchase_orders_line_item pordd ON pordh.purchase_orders_id = pordd.purchase_orders_id
                        LEFT JOIN 	business_partner buss ON pordh.business_partner_id = buss.business_partner_id
                        LEFT JOIN 	purchase_orders_status pordhs ON pordh.purchase_orders_status = pordhs.purchase_orders_status
                        LEFT JOIN 	purchase_orders_note pordn ON pordh.purchase_orders_id = pordn.purchase_orders_id

                        WHERE	    1=1
            --          AND         apptobj.appointment_id='292456'

                        GROUP BY    apptobj.appointment_id
                        ,           apptobj.appt_obj_type
                        ,           CASE 
                                        WHEN pordh.purchase_orders_status <> 950 THEN pordh.tc_purchase_orders_id 
                                        ELSE NULL 
                                    END
                        ,           CASE 
                                        WHEN pordh.purchase_orders_status = 950 THEN pordh.tc_purchase_orders_id 
                                        ELSE NULL 
                                    END
                        ,           pordh.business_partner_id
                        ,           CASE
                                        WHEN SUBSTR(pordn.note,1,2) = 'PU' THEN 1
                                        WHEN SUBSTR(pordn.note,1,2) = 'CU' THEN 2
                                        WHEN SUBSTR(pordn.note,1,2) = 'WU' THEN 3
                                        WHEN SUBSTR(pordn.note,1,2) = 'P0' THEN 4
                                        WHEN SUBSTR(pordn.note,1,2) = 'C0' THEN 5
                                        WHEN SUBSTR(pordn.note,1,2) = 'W0' THEN 6
                                        WHEN SUBSTR(pordn.note,1,2) = 'UR' THEN 7
                                        ELSE 8
                                    END     
            )
            WHERE       1=1
            GROUP BY    appointment_id
            ,           appt_obj_type
)	pordhd ON appth.appointment_id = pordhd.appointment_id

LEFT JOIN   business_partner buss ON pordhd.business_partner_id = buss.business_partner_id

LEFT JOIN   (
                SELECT              appointment_id
                ,                   attribute_value 

                FROM                ilm_appt_custom_attribute

                WHERE               attribute_name='NUM_CNT'
) apptcnt ON appth.appointment_id = apptcnt.appointment_id

LEFT JOIN   (
                SELECT              appointment_id
                ,                   attribute_value as nb_pal 

                FROM                ilm_appt_custom_attribute 

                WHERE               attribute_name='NB_PALETTE'
) apptpal ON appth.appointment_id = apptpal.appointment_id

LEFT JOIN   (
                SELECT              appointment_id
                ,                   attribute_value as nb_package 

                FROM                ilm_appt_custom_attribute 

                WHERE               attribute_name='NBR_COLIS'
) apptpack ON appth.appointment_id = apptpack.appointment_id

LEFT JOIN   (
                SELECT              appointment_id
                ,                   attribute_value as date_arrival

                FROM                ilm_appt_custom_attribute

                WHERE               attribute_name='ARRIVAL_DTTM'
) apptarriv ON appth.appointment_id = apptarriv.appointment_id

LEFT JOIN   (
                SELECT              appointment_id
                ,                   attribute_value 

                FROM                ilm_appt_custom_attribute

                WHERE               attribute_name='SEND_TO_DOCK'
) apptsend ON appth.appointment_id = apptsend.appointment_id

LEFT JOIN   (
                SELECT              appointment_id
                ,                   attribute_value 

                FROM                ilm_appt_custom_attribute

                WHERE               attribute_name='CUST_COMMENTS'
) apptcomm ON appth.appointment_id = apptcomm.appointment_id

LEFT JOIN   ilm_appt_slots islots ON appth.appointment_id = islots.appointment_id
LEFT JOIN   slots ON islots.slot_id = slots.slot_id
LEFT JOIN   (	
                SELECT      trlvd.appointment_id
                ,           trlvd.type
                ,           trlvd.seal_number
                ,           trlvh.checkin_dttm
                ,           trlvh.checkout_dttm
                ,           locnh.dsp_locn
                ,           trlvh.trailer_id
                ,           MIN(taskh.created_dttm) 		as min_task
                ,           CASE 
                                WHEN MAX(taskh.last_updated_dttm) = MIN(taskh.created_dttm) THEN NULL 
                                ELSE MAX(taskh.last_updated_dttm) 
                            END  	                        as max_task

                FROM        trailer_visit_detail trlvd
                LEFT JOIN   trailer_visit trlvh on trlvd.visit_id = trlvh.visit_id
                LEFT JOIN	trailer_ref trlvr ON trlvh.visit_id = trlvr.active_visit_id 
                LEFT JOIN 	locn_hdr locnh ON trlvr.current_location_id = locnh.locn_id
                LEFT JOIN   ilm_tasks taskh ON trlvh.trailer_id = taskh.trailer_id AND trlvh.checkin_dttm <= taskh.created_dttm

                WHERE       1=1
                --AND         trlvd.appointment_id=11355481

                GROUP BY    trlvd.appointment_id
                ,           trlvd.type
                ,           trlvd.seal_number
                ,           trlvh.checkin_dttm
                ,           trlvh.checkout_dttm
                ,           locnh.dsp_locn
                ,           trlvh.trailer_id
) trailerhd ON appth.appointment_id = trailerhd.appointment_id -- AND apptobj.appt_obj_id = trailerhd.po_id

WHERE       1=1
--and appth.tc_appointment_id	in ('11385692','11391826')
--and  slots.description = 'BATIMENT B'

AND		    (pordhd.appt_obj_type = 40 OR pordhd.appt_obj_type IS NULL)
AND		    (
                CASE
                    WHEN (SUBSTR(purchase_orders_running || purchase_orders_closed,1,1) in ('G','C')
                        OR slots.description in ('RDV LOL' , 'RDV DECOCLICO', 'RDV RETOURS CONTENANT/MAGS' )
                        )THEN 0

                    WHEN (SUBSTR(purchase_orders_running || purchase_orders_closed,1,1) = '3'
                            OR 
                            (COALESCE(CAST(apptpal.nb_pal AS INTEGER),0) 
                            + COALESCE(CAST(apptpack.nb_package AS INTEGER),0) 
                            + COALESCE(CAST(pordhd.nb_lines AS INTEGER),0)) > 0
                            )
                        AND slots.description in ( 
                            'BATIMENT A'
                            , 'BATIMENT A (EXTERIEUR)'
                            , 'BATIMENT A (INTERIEUR)'
                            , 'BATIMENT B'
                            , 'BATIMENT B (CAMION)'
                            , 'BATIMENT C'
                            , 'BATIMENT C (CAMION)'
							, 'BATIMENT C (CONTAINER)'
                            , 'BATIMENT C1'
                            )
                        THEN 1
                    ELSE 0
                END
) = 1
-- AND     TRUNC(appth.scheduled_dttm) = TRUNC(SYSDATE)
AND         TRUNC(appth.scheduled_dttm) BETWEEN TRUNC(SYSDATE) 
AND         (
                CASE
                    -- /!\ CASE PARAMETRES REGIONNAUX : POUR LE VENDREDI , affichage du 6 au lieu de 5 a cause de SCI  /!\ 
                    WHEN (1 + TRUNC (SYSDATE) - TRUNC (SYSDATE, 'IW')) = 5 THEN TRUNC(sysdate+3)
                    ELSE TRUNC(sysdate+1)
                END
)

/*AND         ( COALESCE(CAST(apptpal.nb_pal AS INTEGER),0) 
            + COALESCE(CAST(apptpack.nb_package AS INTEGER),0) 
            + COALESCE(CAST(pordhd.nb_lines AS INTEGER),0)) > 0*/

--and (COALESCE(CAST(apptpal.nb_pal  AS DECIMAL(10,3) ),0) = 0 and  COALESCE(CAST(apptpack.nb_package  AS DECIMAL(10,3) ),0)  = 0)
--order by appth.scheduled_dttm