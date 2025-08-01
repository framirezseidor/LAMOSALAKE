---------- Verificar que la dimensión tenga asignado el RAP correcto
SELECT GET_DDL('VIEW', 'CON.VW_DIM_ORG_CANALDISTRIBUCION');
SELECT * FROM CON.VW_DIM_ORG_CANALDISTRIBUCION;
SELECT GET_DDL('VIEW', 'CON.VW_DIM_ORG_SOCIEDAD');
SELECT * FROM CON.VW_DIM_ORG_SOCIEDAD;
SELECT GET_DDL('VIEW', 'CON.VW_DIM_CLI_SOLICITANTE');
SELECT DISTINCT SOLICITANTE_ID, GRUPOCLIENTES2_ID FROM CON.VW_DIM_CLI_SOLICITANTE;
SELECT GET_DDL('VIEW', 'CON.VW_DIM_CLI_DESTINATARIO');
SELECT * FROM CON.VW_DIM_CLI_DESTINATARIO; // No funciona con SAP Role
SELECT GET_DDL('VIEW', 'CON.VW_DIM_ORG_ORGANIZACIONVENTAS');
SELECT * FROM CON.VW_DIM_ORG_ORGANIZACIONVENTAS;


---------- Validar acceso sin roles asignados
-- SELECT * FROM CON.VW_FCT_FIN_ADH_CARTERA;
SELECT * FROM CON.VW_FCT_FIN_REV_CARTERA;



---------- Un rol asignado con la dimensión y un valor de dimensión
INSERT INTO SEGURIDAD.SEG_USUARIO_ROLE
VALUES
    -- ('AGUSTIN_GUTIERREZ', 'CARTERA_ADH_ROLE')
    ('AGUSTIN_GUTIERREZ', 'CARTERA_REV_ROLE')
;
INSERT INTO SEGURIDAD.SEG_ROLE_DIMENSION
VALUES
--ADH
-- ('CARTERA_ADH_ROLE', 'CANALDISTRIB_ID', 'NA'),
-- ('CARTERA_ADH_ROLE', 'SOCIEDAD_ID', 'A201'),
-- ('CARTERA_ADH_ROLE', 'OFICINAVENTAS_ID', 'AGU1')
--REV
('CARTERA_REV_ROLE', 'CANALDISTRIB_ID', 'NA'),
('CARTERA_REV_ROLE', 'SOCIEDAD_ID', 'R101'),
('CARTERA_REV_ROLE', 'ORGVENTAS_ID', 'R311'),
('CARTERA_REV_ROLE', 'COORDINADORDEST_ID', '0001303073'),
('CARTERA_REV_ROLE', 'ASESORDEST_ID', '0001303073'),
('CARTERA_REV_ROLE', 'GRUPOCLIENTES2_ID', 'ZPO')
;
SELECT DISTINCT CANALDISTRIB_ID, SOCIEDAD_ID, ORGVENTAS_ID, DESTINATARIO_ID, SOLICITANTE_ID FROM CON.VW_FCT_FIN_REV_CARTERA;


---------- Un rol asignado con la dimensión y diferentes  valores de dimensión
--ADH
INSERT INTO SEGURIDAD.SEG_ROLE_DIMENSION
VALUES
--ADH
-- ('CARTERA_ADH_ROLE', 'CANALDISTRIB_ID', 'EX'),
-- ('CARTERA_ADH_ROLE', 'SOCIEDAD_ID', 'A101'),
-- ('CARTERA_ADH_ROLE', 'OFICINAVENTAS_ID', 'AEX1')
--REV
('CARTERA_REV_ROLE', 'CANALDISTRIB_ID', 'VD'),
('CARTERA_REV_ROLE', 'SOCIEDAD_ID', 'R401'),
('CARTERA_REV_ROLE', 'ORGVENTAS_ID', 'R314'),
('CARTERA_REV_ROLE', 'COORDINADORDEST_ID', '0001302990'),
('CARTERA_REV_ROLE', 'ASESORDEST_ID', '0001301665'),
('CARTERA_REV_ROLE', 'GRUPOCLIENTES2_ID', 'ZLA')
;
SELECT DISTINCT CANALDISTRIB_ID, SOCIEDAD_ID, ORGVENTAS_ID, DESTINATARIO_ID, SOLICITANTE_ID FROM CON.VW_FCT_FIN_REV_CARTERA;


---------- Rol SAP_ROLE asignado
DELETE FROM SEGURIDAD.SEG_ROLE_DIMENSION
WHERE ROLE_NM LIKE 'CARTERA%';


INSERT INTO SEGURIDAD.SEG_USUARIO_ROLE
VALUES
    ('AGUSTIN_GUTIERREZ', 'SAP_ROLE');
SELECT * FROM CON.FCT_FIN_CARTERAACT
WHERE 
    CANALDISTRIB_ID = 'VD'
    SOCIEDAD_ID = 'R401'
    ORGVENTAS_ID = 'R314'
    COVAR_POP = '0001302990'
    ASESORDEST_ID', '0001301665
    GRUPOCLIENTES2_ID', 'ZLA;

SELECT DISTINCT CANALDISTRIB_ID, SOCIEDAD_ID, ORGVENTAS_ID, DESTINATARIO_ID, SOLICITANTE_ID FROM CON.VW_FCT_FIN_ADH_CARTERA;

SELECT * FROM CON.DIM_TRA_TIPOVEHICULO;

---------- Dimensión asignada a Role con valor ALL
DELETE FROM SEGURIDAD.SEG_USUARIO_ROLE
WHERE USER_NM = 'AGUSTIN_GUTIERREZ'
-- AND ROLE_NM = 'SAP_ROLE'
;

INSERT INTO SEGURIDAD.SEG_ROLE_DIMENSION
VALUES
--ADH
-- ('CARTERA_ADH_ROLE', 'CANALDISTRIB_ID', 'ALL'),
-- ('CARTERA_ADH_ROLE', 'SOCIEDAD_ID', 'ALL'),
-- ('CARTERA_ADH_ROLE', 'OFICINAVENTAS_ID', 'ALL')
-- REV
('CARTERA_REV_ROLE', 'CANALDISTRIB_ID', 'ALL'),
('CARTERA_REV_ROLE', 'SOCIEDAD_ID', 'ALL'),
('CARTERA_REV_ROLE', 'ORGVENTAS_ID', 'ALL'),
('CARTERA_REV_ROLE', 'COORDINADORDEST_ID', 'ALL'),
('CARTERA_REV_ROLE', 'ASESORDEST_ID', 'ALL'),
('CARTERA_REV_ROLE', 'GRUPOCLIENTES2_ID', 'ALL')
;
SELECT * FROM CON.VW_FCT_FIN_ADH_CARTERA;


---------- Dimensión asignada a Usuario con valor ALL -- NO APLICA
DELETE FROM SEGURIDAD.SEG_ROLE_DIMENSION
WHERE ROLE_NM LIKE 'CARTERA%';
INSERT INTO SEGURIDAD.SEG_USUARIO_DIMENSION
VALUES 
('AGUSTIN_GUTIERREZ', 'CANALDISTRIB_ID', 'ALL'),
('AGUSTIN_GUTIERREZ', 'SOCIEDAD_ID', 'ALL'),
-- ('AGUSTIN_GUTIERREZ', 'OFICINAVENTAS_ID', 'ALL')
('AGUSTIN_GUTIERREZ', 'ORGVENTAS_ID', 'ALL'),
('AGUSTIN_GUTIERREZ', 'GRUPOCLIENTES2_ID', 'ALL'),
('AGUSTIN_GUTIERREZ', 'COORDINADORDEST_ID', 'ALL'),
('AGUSTIN_GUTIERREZ', 'ASESORDEST_ID', 'ALL')
;
SELECT * FROM CON.VW_FCT_FIN_REV_CARTERA;


----------------------------------------
DELETE FROM SEGURIDAD.SEG_ROLE_DIMENSION
WHERE ROLE_NM = 'CARTERA_REV_ROLE'
;


----------------------------------------------------

SELECT CANALDISTRIB_ID, ORGVENTAS_ID, SOCIEDAD_ID, DESTINATARIO_ID, SOLICITANTE_ID, count(*) FROM CON.FCT_FIN_CARTERAHIST
WHERE SOCIEDAD_ID LIKE 'R%'
GROUP BY 1,2,3,4,5
ORDER BY 6 DESC;

-- CANAL: NA
-- SOCIEDAD: R101
-- ORGVENTAS: R311
-- DESTINATARIO: R311_NA_00_10200120
-- SOLICITANTE: R311_NA_00_10100024
-- ----
-- CANAL: VD
-- SOCIEDAD: R401
-- ORGVENTAS: R314
-- DESTINATARIO: R314_VD_00_10104423
-- SOLICITANTE: R314_VD_00_10104423

	

SELECT DESTINATARIO_ID, COORDINADORDEST_ID, ASESORDEST_ID FROM CON.DIM_CLI_DESTINATARIO 
WHERE COORDINADORDEST_ID = '0001303073' AND ASESORDEST_ID = '0001303073';

SELECT SOLICITANTE_ID, GRUPOCLIENTES2_ID FROM CON.DIM_CLI_SOLICITANTE
WHERE SOLICITANTE_ID IN ('R311_NA_00_10100024', 'R314_VD_00_10104423');

SELECT DISTINCT
    SOLICITANTE_ID,
    DESTINATARIO_ID
FROM
    CON.VW_FCT_FIN_REV_CARTERA;


SELECT COUNT(DISTINCT DIM_ROL)
              FROM SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION
              WHERE 
                USUARIO = CURRENT_USER();
                AND DIM_ROL IN ('COORDINADORDEST_ID', 'ASESORDEST_ID');

SELECT *
              FROM SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION
            --   WHERE 
                -- USUARIO = 'CURRENT_USER()'
                ;