CREATE OR REPLACE ROW ACCESS POLICY SEGURIDAD.RAP_SAP_DIM_ASESORCOMERCIAL_ID
AS (ASESORCOMERCIAL_ID varchar(50))
RETURNS BOOLEAN ->
CASE
    WHEN (SELECT SEGURIDAD.FN_RAP_DIM(ASESORCOMERCIAL_ID , 'ASESORCOMERCIAL_ID'))=TRUE
    THEN TRUE
ELSE
    FALSE
END
;


