CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_MAT_MATERIALVENTAS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-04-28
 Creador:            Juan Diego Bonifacio
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_MAT_MATERIALVENTAS
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    T_EJECUCION     NUMBER(38,0);
    ROWS_INSERTED   NUMBER(38,0);
    TEXTO           VARCHAR(200);

BEGIN

    ---------------------------------------------------------------------------------
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM CON.DIM_MAT_MATERIALVENTAS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE PRE HACIA CON
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO CON.DIM_MAT_MATERIALVENTAS
        (
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            GRUPOIMPUTACION_ID,
            GRUPOMATERIALES_ID,
            GRUPOMATERIALES1_ID,
            GRUPOMATERIALES2_ID,
            GRUPOMATERIALES3_ID,
            GRUPOMATERIALES4_ID,
            GRUPOMATERIALES5_ID,
            STATUSMAT_CANDIS_ID,
            MATERIAL_ID,
            MATERIALVENTAS_ID,
            MATERIALVENTAS_TEXT,
            SISORIGEN_ID,
            MANDANTE ,
            FECHA_CARGA ,
            ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde PRE
            ORGVENTAS_ID,
            CANALDISTRIB_ID,
            GRUPOIMPUTACION_ID,
            GRUPOMATERIALES_ID,
            GRUPOMATERIALES1_ID,
            GRUPOMATERIALES2_ID,
            GRUPOMATERIALES3_ID,
            GRUPOMATERIALES4_ID,
            GRUPOMATERIALES5_ID,
            STATUSMAT_CANDIS_ID,
            LTRIM(MATERIAL_ID,'0') MATERIAL_ID,
            MATERIALVENTAS_ID,
            MATERIALVENTAS_TEXT,
            SISORIGEN_ID,
            MANDANTE ,
            FECHA_CARGA ,
            ZONA_HORARIA 
        FROM PRE.PDIM_MAT_MATERIALVENTAS;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_MAT_MATERIALVENTAS;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 3: LOG
    ---------------------------------------------------------------------------------
    SELECT COALESCE(:TEXTO, 'EJECUCION CORRECTA') INTO :TEXTO;

    INSERT INTO LOGS.HISTORIAL_EJECUCIONES
    VALUES ('SP_CON_DIM_MAT_MATERIALVENTAS','CON.DIM_MAT_MATERIALVENTAS', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: CLONNING
    ---------------------------------------------------------------------------------
    DELETE FROM MIRRORING.DIM_MAT_MATERIALVENTAS_ADH;

        INSERT INTO MIRRORING.DIM_MAT_MATERIALVENTAS_ADH
        (
            MATERIALVENTAS_ID	,
            ORGVENTAS_ID	,
            CANALDISTRIB_ID	,
            MATERIAL_ID	,
            GRUPOIMPUTACION_ID	,
            GRUPOMATERIALES_ID	,
            GRUPOMATERIALES1_ID	,
            GRUPOMATERIALES2_ID	,
            GRUPOMATERIALES3_ID	,
            GRUPOMATERIALES4_ID	,
            GRUPOMATERIALES5_ID	,
            STATUSMAT_CANDIS_ID	,
            MATERIALVENTAS_TEXT	,
            ORGVENTAS_TEXT	,
            CANALDISTRIB_TEXT	,
            GRUPOIMPUTACION_TEXT	,
            GRUPOMATERIALES_TEXT	,
            GRUPOMATERIALES1_TEXT	,
            GRUPOMATERIALES2_TEXT	,
            GRUPOMATERIALES3_TEXT	,
            GRUPOMATERIALES4_TEXT	,
            GRUPOMATERIALES5_TEXT	,
            STATUSMAT_CANDIS_TEXT	,
            MATERIALVENTAS_ID_TEXT	,
            ORGVENTAS_ID_TEXT	,
            CANALDISTRIB_ID_TEXT	,
            GRUPOIMPUTACION_ID_TEXT	,
            GRUPOMATERIALES_ID_TEXT	,
            GRUPOMATERIALES1_ID_TEXT	,
            GRUPOMATERIALES2_ID_TEXT	,
            GRUPOMATERIALES3_ID_TEXT	,
            GRUPOMATERIALES4_ID_TEXT	,
            GRUPOMATERIALES5_ID_TEXT	,
            STATUSMAT_CANDIS_ID_TEXT	,
            SISORIGEN_ID	,
            MANDANTE	,
            FECHA_CARGA	,
            ZONA_HORARIA	

        )
        SELECT
            MV.MATERIALVENTAS_ID	,
            MV.ORGVENTAS_ID	,
            MV.CANALDISTRIB_ID	,
            MV.MATERIAL_ID	,
            MV.GRUPOIMPUTACION_ID	,
            MV.GRUPOMATERIALES_ID	,
            MV.GRUPOMATERIALES1_ID	,
            MV.GRUPOMATERIALES2_ID	,
            MV.GRUPOMATERIALES3_ID	,
            MV.GRUPOMATERIALES4_ID	,
            MV.GRUPOMATERIALES5_ID	,
            MV.STATUSMAT_CANDIS_ID	,
            MV.MATERIALVENTAS_TEXT	,
            COALESCE(OV.ORGVENTAS_TEXT,'') ORGVENTAS_TEXT,
            COALESCE(CD.CANALDISTRIB_TEXT,'') CANALDISTRIB_TEXT,
            COALESCE(GI.GRUPOIMPUTACION_TEXT,'') GRUPOIMPUTACION_TEXT,
            COALESCE(GM.GRUPOMATERIALES_TEXT,'') GRUPOMATERIALES_TEXT,
            COALESCE(GM1.GRUPOMATERIALES1_TEXT, '') GRUPOMATERIALES1_TEXT ,
            COALESCE(GM2.GRUPOMATERIALES2_TEXT, '') GRUPOMATERIALES2_TEXT ,
            COALESCE(GM3.GRUPOMATERIALES3_TEXT, '') GRUPOMATERIALES3_TEXT ,
            COALESCE(GM4.GRUPOMATERIALES4_TEXT, '') GRUPOMATERIALES4_TEXT ,
            COALESCE(GM5.GRUPOMATERIALES5_TEXT, '') GRUPOMATERIALES5_TEXT ,
            COALESCE(SM.STATUSMATERIAL_TEXT,'') STATUSMAT_CANDIS_TEXT,
            CONCAT(MV.MATERIALVENTAS_ID, ' - ',  MV.MATERIALVENTAS_TEXT ) MATERIALVENTAS_ID_TEXT,
            CONCAT(MV.ORGVENTAS_ID, ' - ', COALESCE(OV.ORGVENTAS_TEXT,'')) ORGVENTAS_ID_TEXT,
            CONCAT(MV.CANALDISTRIB_ID, ' - ', COALESCE(CD.CANALDISTRIB_TEXT,'') ) CANALDISTRIB_ID_TEXT,
            CONCAT(MV.GRUPOIMPUTACION_ID, ' - ',  COALESCE(GI.GRUPOIMPUTACION_TEXT,'')) GRUPOIMPUTACION_ID_TEXT,
            CONCAT(MV.GRUPOMATERIALES_ID, ' - ', COALESCE(GM.GRUPOMATERIALES_TEXT,'') ) GRUPOMATERIALES_ID_TEXT,
            CONCAT(MV.GRUPOMATERIALES1_ID, ' - ', COALESCE(GM1.GRUPOMATERIALES1_TEXT,'') ) GRUPOMATERIALES1_ID_TEXT,
            CONCAT(MV.GRUPOMATERIALES2_ID, ' - ', COALESCE(GM2.GRUPOMATERIALES2_TEXT,'') ) GRUPOMATERIALES2_ID_TEXT,
            CONCAT(MV.GRUPOMATERIALES3_ID, ' - ', COALESCE(GM3.GRUPOMATERIALES3_TEXT,'') ) GRUPOMATERIALES3_ID_TEXT,
            CONCAT(MV.GRUPOMATERIALES4_ID, ' - ', COALESCE(GM4.GRUPOMATERIALES4_TEXT,'') ) GRUPOMATERIALES4_ID_TEXT,
            CONCAT(MV.GRUPOMATERIALES5_ID, ' - ', COALESCE(GM5.GRUPOMATERIALES5_TEXT,'') ) GRUPOMATERIALES5_ID_TEXT,
            CONCAT(MV.STATUSMAT_CANDIS_ID, ' - ', COALESCE(SM.STATUSMATERIAL_TEXT,''))  STATUSMAT_CANDIS_ID_TEXT,
            MV.SISORIGEN_ID,
            MV.MANDANTE ,
            MV.FECHA_CARGA ,
            MV.ZONA_HORARIA 
        FROM CON.DIM_MAT_MATERIALVENTAS MV
            LEFT JOIN CON.DIM_ORG_ORGANIZACIONVENTAS OV
            ON MV.ORGVENTAS_ID = OV.ORGVENTAS_ID
            LEFT JOIN CON.DIM_ORG_CANALDISTRIBUCION CD
            ON MV.CANALDISTRIB_ID = CD.CANALDISTRIB_ID
            LEFT JOIN CON.DIM_MAT_GRUPOIMPUTACION GI
            ON MV.GRUPOIMPUTACION_ID = GI.GRUPOIMPUTACION_ID
            LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES GM
            ON MV.GRUPOMATERIALES_ID = GM.GRUPOMATERIALES_ID
            LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES1 GM1
            ON MV.GRUPOMATERIALES1_ID = GM1.GRUPOMATERIALES1_ID
            LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES2 GM2
            ON MV.GRUPOMATERIALES2_ID = GM2.GRUPOMATERIALES2_ID
            LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES3 GM3
            ON MV.GRUPOMATERIALES3_ID = GM3.GRUPOMATERIALES3_ID
            LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES4 GM4
            ON MV.GRUPOMATERIALES4_ID = GM4.GRUPOMATERIALES4_ID
            LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES5 GM5
            ON MV.GRUPOMATERIALES5_ID = GM5.GRUPOMATERIALES5_ID
            LEFT JOIN CON.DIM_MAT_STATUSMATERIAL SM
            ON MV.STATUSMAT_CANDIS_ID = SM.STATUSMATERIAL_ID
            ;



    ---------------------------------------------------------------------------------
    -- STEP 5: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;

select distinct statusmat_candis_id, statusmat_candis_text,statusmat_candis_id_text, count(*) from MIRRORING.DIM_MAT_MATERIALVENTAS_ADH group by statusmat_candis_id, statusmat_candis_text,statusmat_candis_id_text;

select * from CON.DIM_MAT_STATUSMATERIAL SM;

select distinct STATUSMAT_CANDIS_ID from CON.DIM_MAT_MATERIALVENTAS MV;