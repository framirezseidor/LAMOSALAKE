CREATE OR REPLACE PROCEDURE PRE.SP_PRE_PDIM_MAT_MATERIAL()
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
 Descripción:        SP que transforma datos desde la capa RAW a PRE para PDIM_MAT_MATERIAL
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
    -- STEP 1: LIMPIEZA DE DATOS EN LA TABLA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        DELETE FROM PRE.PDIM_MAT_MATERIAL;

        SELECT CURRENT_TIMESTAMP() INTO :F_FIN;
        SELECT DATEDIFF(millisecond, :F_INICIO, :F_FIN) INTO :T_EJECUCION;
    EXCEPTION
        WHEN statement_error THEN
            SELECT ('Error en DELETE: ' || :sqlerrm) INTO :TEXTO;
    END;

    ---------------------------------------------------------------------------------
    -- STEP 2: INSERCIÓN DE DATOS DESDE RAW HACIA PRE
    ---------------------------------------------------------------------------------
    BEGIN
        SELECT CURRENT_TIMESTAMP() INTO :F_INICIO;

        INSERT INTO PRE.PDIM_MAT_MATERIAL
        (
            MATERIAL_ID,
            TIPOMATERIAL_ID,
            GRUPOARTICULOS_ID,
            STATUSMAT_TODOSCENTROS_ID,
            STATUSMAT_CANDIS_ID,
            UNI_UMB,
            COLOR_ADH_ID,
            COLORGRUPO_ADH_ID,
            LINEAMATERIAL_ADH_ID,
            MARCA_ADH_ID,
            MARCAGRUPO_ADH_ID,
            NIVELPRECIO_ADH_ID,
            ORIGENMATERIAL_ADH_ID,
            ORIGENPAIS_ADH_ID,
            PRESENTACION_ID,
            FAMILIA_ADH_ID,
            TIPOEMPAQUE_ID,
            ACABADO_ID,
            APLICACION_ID,
            DISENODETALLE_ID,
            CALIDAD_ID,
            LANZAMIENTOS_ID,
            COLOR_REV_ID,
            COLORGRUPO_REV_ID,
            CUERPO_ID,
            CUERPODET_ID,
            DISENO_ID,
            ESPESOR_ID,
            ESTATUSCATALOGO_ID,
            FAMILIA_REV_ID,
            FAMILIAGRUPO_ID,
            FORMATO_ID,
            FORMATOGRUPO_ID,
            FORMATORANGO_ID,
            FORMATOTAMANO_ID,
            LINEAMATERIAL_ID,
            MARCA_REV_ID,
            MARCAGRUPO_REV_ID,
            MARCAPLANEACION_ID,
            SEGMENTO_REV_ID,
            PEIDESTONALIZACION_ID,
            ORIGENMATERIAL_REV_ID,
            PADRE_ID,
            ORIGENPAIS_REV_ID,
            PROCESOADICIONAL_ID,
            ORIGENPROVEEDOR_ID,
            PUNZON_ID,
            PUNZONDETALLE_ID,
            RECTIFICADO_ID,
            RESERVADO_ID,
            SUBMARCA_ID,
            TECNOLOGIA_ID,
            TECNOLOGIAGRUPO_ID,
            MATERIAL_TEXT,
            SISORIGEN_ID,
            MANDANTE,
            FECHA_CARGA ,
            ZONA_HORARIA
        )
        SELECT
            -- Transformaciones desde RAW
            ATTR.MATNR AS MATERIAL_ID,
            ATTR.MTART AS TIPOMATERIAL_ID,
            ATTR.MATKL AS GRUPOARTICULOS_ID,
            ATTR.MSTAE AS STATUSMAT_TODOSCENTROS_ID,
            ATTR.MSTAV AS STATUSMAT_CANDIS_ID,
            ATTR.MEINS AS UNI_UMB,
            OMAT.A_COLOR001 AS COLOR_ADH_ID,
            OMAT.A_COLOR_GRUP002 AS COLORGRUPO_ADH_ID,
            OMAT.A_LINEA_MATE001 AS LINEAMATERIAL_ADH_ID,
            OMAT.A_MARCA002 AS MARCA_ADH_ID,
            OMAT.A_MARCA_GRUP001 AS MARCAGRUPO_ADH_ID,
            OMAT.A_NIVEL_PREC001 AS NIVELPRECIO_ADH_ID,
            OMAT.A_ORIGEN_MAT001 AS ORIGENMATERIAL_ADH_ID,
            OMAT.A_PAIS_ORIGE002 AS ORIGENPAIS_ADH_ID,
            OMAT.A_PRESENTACI001 AS PRESENTACION_ID,
            OMAT.A_SUBLINEA_M001 AS FAMILIA_ADH_ID,
            OMAT.A_TIPO_EMPAQ001 AS TIPOEMPAQUE_ID,
            OMAT.R_ACABADO001 AS ACABADO_ID,
            OMAT.R_APLICACION002 AS APLICACION_ID,
            OMAT.R_APLICACION003 AS DISENODETALLE_ID,
            OMAT.R_CALIDAD002 AS CALIDAD_ID,
            OMAT.R_COLECCION004 AS LANZAMIENTOS_ID,
            OMAT.R_COLOR005 AS COLOR_REV_ID,
            OMAT.R_COLOR_GRUP003 AS COLORGRUPO_REV_ID,
            OMAT.R_CUERPO001 AS CUERPO_ID,
            OMAT.R_CUERPO_DET006 AS CUERPODET_ID,
            OMAT.R_DISENO001 AS DISENO_ID,
            OMAT.R_ESPESOR002 AS ESPESOR_ID,
            OMAT.R_ESTATUS_CA001 AS ESTATUSCATALOGO_ID,
            OMAT.R_FAMILIA003 AS FAMILIA_REV_ID,
            OMAT.R_FAMILIA_GR002 AS FAMILIAGRUPO_ID,
            OMAT.R_FORMATO004 AS FORMATO_ID,
            OMAT.R_FORMATO_GR001 AS FORMATOGRUPO_ID,
            OMAT.R_FORMATO_RA006 AS FORMATORANGO_ID,
            OMAT.R_FORMATO_TA005 AS FORMATOTAMANO_ID,
            OMAT.R_LINEA_MATE001 AS LINEAMATERIAL_ID,
            OMAT.R_MARCA001 AS MARCA_REV_ID,
            OMAT.R_MARCA_GRUP003 AS MARCAGRUPO_REV_ID,
            OMAT.R_MARCA_PLAN002 AS MARCAPLANEACION_ID,
            OMAT.R_NIVEL_PREC001 AS SEGMENTO_REV_ID,
            OMAT.R_NIVEL_PREC002 AS PEIDESTONALIZACION_ID,
            OMAT.R_ORIGEN_MAT001 AS ORIGENMATERIAL_REV_ID,
            OMAT.R_PADRE006 AS PADRE_ID,
            OMAT.R_PAIS_ORIGE001 AS ORIGENPAIS_REV_ID,
            OMAT.R_PROADICION005 AS PROCESOADICIONAL_ID,
            OMAT.R_PROVEEDOR_002 AS ORIGENPROVEEDOR_ID,
            OMAT.R_PUNZON004 AS PUNZON_ID,
            OMAT.R_PUNZON_DET003 AS PUNZONDETALLE_ID,
            OMAT.R_RECTIFICAD001 AS RECTIFICADO_ID,
            OMAT.R_SUBLINEA_M002 AS RESERVADO_ID,
            OMAT.R_SUBMARCA001 AS SUBMARCA_ID,
            OMAT.R_TECNOLOGIA001 AS TECNOLOGIA_ID,
            OMAT.R_TECNOLOGIA002 AS TECNOLOGIAGRUPO_ID,
            TEXT.TXTMD AS MATERIAL_TEXT,
            ATTR.SISORIGEN_ID,
            ATTR.MANDANTE,
            CURRENT_TIMESTAMP AS FECHA_CARGA,
            RIGHT(CURRENT_TIMESTAMP,5) AS ZONA_HORARIA 
        FROM RAW.SQ1_EXT_0MATERIAL_ATTR AS ATTR 
        LEFT JOIN RAW.SQ1_EXT_0MATERIAL_TEXT AS TEXT 
        ON ATTR.MATNR = TEXT.MATNR 
        LEFT JOIN RAW.SQ1_EXT_1CL_OMAT001 AS OMAT
        ON ATTR.MATNR = OMAT.MATNR
        WHERE TEXT.SPRAS= 'S';

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM PRE.PDIM_MAT_MATERIAL;

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
    VALUES ('SP_PRE_PDIM_MAT_MATERIAL','PRE.PDIM_MAT_MATERIAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: FINALIZACIÓN
    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 