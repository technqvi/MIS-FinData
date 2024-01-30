WITH last_incident_of_id AS ( SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY ImportedAt DESC) AS row_num
    FROM SMartDW.view_demo_incident_inventory
)
SELECT * EXCEPT (row_num) FROM  last_incident_of_id WHERE row_num = 1;