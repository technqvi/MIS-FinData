create view view_demo_incident_inventory as
SELECT incident.id,
       to_timestamp(
               to_char((incident.incident_datetime AT TIME ZONE 'Asia/Bangkok'::text), 'YYYY-MM-DD HH24:MI:SS'::text),
               'YYYY-MM-DD HH24:MI:SS'::text)::timestamp without time zone                                          AS incident_datetime,
       to_char((incident.incident_close_datetime AT TIME ZONE 'Asia/Bangkok'::text),
               'YYYY-MM-DD HH24:MI:SS'::text)                                                                       AS close_datetime,
       (SELECT x.incident_type_name
        FROM app_incident_type x
        WHERE x.id = incident.incident_type_id)                                                                     AS incident_type,
       (SELECT x.severity_name
        FROM app_incident_severity x
        WHERE x.id = incident.incident_severity_id)                                                                 AS severity,
       brand.brand_name                                                                                             AS brand,
       model.model_name                                                                                             AS model,
	   
	   
	          to_timestamp(
               to_char((incident.updated_at   AT TIME ZONE 'Asia/Bangkok'::text), 'YYYY-MM-DD HH24:MI:SS'::text),
               'YYYY-MM-DD HH24:MI:SS'::text)::timestamp without time zone     as last_update_date,     
	   
	   
	          to_timestamp(
               to_char((now() AT TIME ZONE 'Asia/Bangkok'::text), 'YYYY-MM-DD HH24:MI:SS'::text),
               'YYYY-MM-DD HH24:MI:SS'::text)::timestamp without time zone    as     creation_date 

	   
FROM app_incident incident
         JOIN app_inventory inventory ON incident.inventory_id = inventory.id
         JOIN app_brand brand ON inventory.brand_id = brand.id
         JOIN app_model model ON inventory.model_id = model.id
         JOIN app_product_type apt ON inventory.product_type_id = apt.id;

alter table view_demo_incident_inventory
    owner to postgres;


