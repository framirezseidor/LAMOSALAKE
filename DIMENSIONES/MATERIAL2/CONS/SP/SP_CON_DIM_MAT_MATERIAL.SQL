CREATE OR REPLACE PROCEDURE CON.SP_CON_DIM_MAT_MATERIAL()
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
 Descripción:        SP que transforma datos desde la capa PRE a CON para DIM_MAT_MATERIAL
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

        DELETE FROM CON.DIM_MAT_MATERIAL;

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

        INSERT INTO CON.DIM_MAT_MATERIAL
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
            -- Transformaciones desde PRE
            LTRIM(MATERIAL_ID,'0') MATERIAL_ID,
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
        FROM PRE.PDIM_MAT_MATERIAL;

        -- Conteo de filas insertadas
        SELECT COUNT(*) INTO :ROWS_INSERTED FROM CON.DIM_MAT_MATERIAL;

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
    VALUES ('SP_CON_DIM_MAT_MATERIAL','CON.DIM_MAT_MATERIAL', :F_INICIO, :F_FIN, :T_EJECUCION, :ROWS_INSERTED, :TEXTO );

    ---------------------------------------------------------------------------------
    -- STEP 4: MIRRORING
    ---------------------------------------------------------------------------------
    DELETE FROM MIRRORING.DIM_MAT_MATERIAL_ADH;

    INSERT INTO MIRRORING.DIM_MAT_MATERIAL_ADH
    (
        MATERIAL_ID	,
        TIPOMATERIAL_ID	,
        GRUPOARTICULOS_ID	,
        STATUSMAT_TODOSCENTROS_ID	,
        UNI_UMB	,
        COLOR_ADH_ID	,
        COLORGRUPO_ADH_ID	,
        LINEAMATERIAL_ADH_ID	,
        SUBLINEAMATERIAL_ADH_ID	,
        MARCA_ADH_ID	,
        MARCAGRUPO_ADH_ID	,
        NIVELPRECIO_ADH_ID	,
        ORIGENMATERIAL_ADH_ID	,
        ORIGENPAIS_ADH_ID	,
        PRESENTACION_ID	,
        FAMILIA_ADH_ID	,
        TIPOEMPAQUE_ID	,
        MATERIAL_TEXT	,
        TIPOMATERIAL_TEXT	,
        GRUPOARTICULOS_TEXT	,
        STATUSMAT_TODOSCENTROS_TEXT	,
        UNI_UMB_TEXT	,
        COLOR_ADH_TEXT	,
        COLORGRUPO_ADH_TEXT	,
        LINEAMATERIAL_ADH_TEXT	,
        SUBLINEAMATERIAL_ADH_TEXT	,
        MARCA_ADH_TEXT	,
        MARCAGRUPO_ADH_TEXT	,
        NIVELPRECIO_ADH_TEXT	,
        ORIGENMATERIAL_ADH_TEXT	,
        ORIGENPAIS_ADH_TEXT	,
        PRESENTACION_TEXT	,
        FAMILIA_ADH_TEXT	,
        TIPOEMPAQUE_TEXT	,
        MATERIAL_ID_TEXT	,
        TIPOMATERIAL_ID_TEXT	,
        GRUPOARTICULOS_ID_TEXT	,
        STATUSMAT_TODOSCENTROS_ID_TEXT	,
        UNI_UMB_ID_TEXT	,
        COLOR_ADH_ID_TEXT	,
        COLORGRUPO_ADH_ID_TEXT	,
        LINEAMATERIAL_ADH_ID_TEXT	,
        SUBLINEAMATERIAL_ADH_ID_TEXT	,
        MARCA_ADH_ID_TEXT	,
        MARCAGRUPO_ADH_ID_TEXT	,
        NIVELPRECIO_ADH_ID_TEXT	,
        ORIGENMATERIAL_ADH_ID_TEXT	,
        ORIGENPAIS_ADH_ID_TEXT	,
        PRESENTACION_ID_TEXT	,
        FAMILIA_ADH_ID_TEXT	,
        TIPOEMPAQUE_ID_TEXT	,
        SISORIGEN_ID	,
        FECHA_CARGA	,
        ZONA_HORARIA	,
        MANDANTE	
    )
    SELECT
        LTRIM(M.MATERIAL_ID,'0') MATERIAL_ID	,
        M.TIPOMATERIAL_ID	,
        M.GRUPOARTICULOS_ID	,
        M.STATUSMAT_TODOSCENTROS_ID	,
        M.UNI_UMB	,
        M.COLOR_ADH_ID	,
        M.COLORGRUPO_ADH_ID	,
        M.LINEAMATERIAL_ADH_ID	,
        SL.A_SUBLINEA_M001 SUBLINEAMATERIAL_ADH_ID	,
        M.MARCA_ADH_ID	,
        M.MARCAGRUPO_ADH_ID	,
        M.NIVELPRECIO_ADH_ID	,
        M.ORIGENMATERIAL_ADH_ID	,
        M.ORIGENPAIS_ADH_ID	,
        M.PRESENTACION_ID	,
        M.FAMILIA_ADH_ID	,
        M.TIPOEMPAQUE_ID	,
        M.MATERIAL_TEXT	,
        TM.TIPOMATERIAL_TEXT	,
        GA.GRUPOARTICULOS_TEXT	,
        SM.STATUSMATERIAL_TEXT STATUSMAT_TODOSCENTROS_TEXT	,
        '' UNI_UMB_TEXT	,
        CL.COLOR_ADH_TEXT ,
        CG.COLORGRUPO_ADH_TEXT	,
        LM.LINEAMATERIAL_TEXT	,
        FAMILIA_ADH_TEXT SUBLINEAMATERIAL_ADH_TEXT	,
        MAR.MARCA_ADH_TEXT	,
        MG.MARCAGRUPO_ADH_TEXT	,
        NP.NIVELPRECIO_ADH_TEXT	,
        OM.ORIGENMATERIAL_ADH_TEXT	,
        OP.ORIGENPAIS_ADH_TEXT	,
        PR.PRESENTACION_TEXT	,
        FM.FAMILIA_ADH_TEXT	,
        TE.TIPOEMPAQUE_TEXT	,

        CONCAT(LTRIM(M.MATERIAL_ID,'0'), ' - ', M.MATERIAL_TEXT) MATERIAL_ID_TEXT	,
        CONCAT(M.TIPOMATERIAL_ID, ' - ', TM.TIPOMATERIAL_TEXT) TIPOMATERIAL_ID_TEXT	,
        CONCAT(M.GRUPOARTICULOS_ID, ' - ', GA.GRUPOARTICULOS_TEXT) GRUPOARTICULOS_ID_TEXT	,
        CONCAT(M.STATUSMAT_TODOSCENTROS_ID	, ' - ', SM.STATUSMATERIAL_TEXT) STATUSMAT_TODOSCENTROS_ID_TEXT	,
        '' UNI_UMB_ID_TEXT	,
        CONCAT(COALESCE(M.COLOR_ADH_ID,'')          ,' - ', COALESCE(CL.COLOR_ADH_TEXT,'') ) COLOR_ADH_ID_TEXT ,
        CONCAT(COALESCE(M.COLORGRUPO_ADH_ID,'')     ,' - ', COALESCE(CG.COLORGRUPO_ADH_TEXT,'') ) COLORGRUPO_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.LINEAMATERIAL_ADH_ID,'')  ,' - ', COALESCE(LM.LINEAMATERIAL_TEXT, '') )  LINEAMATERIAL_ADH_ID_TEXT	,
        CONCAT(COALESCE(SL.A_SUBLINEA_M001,'')      ,' - ', COALESCE(FAMILIA_ADH_TEXT,'') ) SUBLINEAMATERIAL_ADH_ID_TEXT,
        CONCAT(COALESCE(M.MARCA_ADH_ID,'')          ,' - ', COALESCE(MAR.MARCA_ADH_TEXT,'') ) MARCA_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.MARCAGRUPO_ADH_ID,'')     ,' - ', COALESCE(MG.MARCAGRUPO_ADH_TEXT,'') ) MARCAGRUPO_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.NIVELPRECIO_ADH_ID,'')    ,' - ', COALESCE(NP.NIVELPRECIO_ADH_TEXT,'') ) NIVELPRECIO_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.ORIGENMATERIAL_ADH_ID,'') ,' - ', COALESCE(OM.ORIGENMATERIAL_ADH_TEXT,'') ) ORIGENMATERIAL_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.ORIGENPAIS_ADH_ID,'')     ,' - ', COALESCE(OP.ORIGENPAIS_ADH_TEXT,'') ) ORIGENPAIS_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.PRESENTACION_ID,'')       ,' - ', COALESCE(PR.PRESENTACION_TEXT,'') ) PRESENTACION_ID_TEXT	,
        CONCAT(COALESCE(M.FAMILIA_ADH_ID,'')        ,' - ', COALESCE(FM.FAMILIA_ADH_TEXT,'') ) FAMILIA_ADH_ID_TEXT	,
        CONCAT(COALESCE(M.TIPOEMPAQUE_ID,'')        ,' - ', COALESCE(TE.TIPOEMPAQUE_TEXT,'') ) TIPOEMPAQUE_ID_TEXT	, 
        M.SISORIGEN_ID	,
        M.FECHA_CARGA	,
        M.ZONA_HORARIA	,
        M.MANDANTE	
    FROM
        CON.DIM_MAT_MATERIAL M
        LEFT JOIN CON.DIM_MAT_TIPOMATERIAL TM
        ON M.TIPOMATERIAL_ID = TM.TIPOMATERIAL_ID
        LEFT JOIN CON.DIM_MAT_GRUPOARTICULOS GA
        ON M.GRUPOARTICULOS_ID = GA.GRUPOARTICULOS_ID
        LEFT JOIN CON.DIM_MAT_STATUSMATERIAL SM
        ON M.STATUSMAT_TODOSCENTROS_ID = SM.STATUSMATERIAL_ID
        LEFT JOIN CON.DIM_MAT_COLOR_ADH CL
        ON M.COLOR_ADH_ID = CL.COLOR_ADH_ID
        LEFT JOIN CON.DIM_MAT_COLORGRUPO_ADH CG
        ON M.COLORGRUPO_ADH_ID = CG.COLORGRUPO_ADH_ID
        LEFT JOIN CON.DIM_MAT_LINEAMATERIAL_ADH LM
        ON M.LINEAMATERIAL_ADH_ID = LM.LINEAMATERIAL_ID
        LEFT JOIN RAW.SQ1_EXT_1CL_OMAT001 SL
        ON M.MATERIAL_ID = SL.MATNR
    --    --LEFT JOIN CON.DIM_MAT_SUBLINEAMATERIAL_ADH SLM
    --    --ON SL.A_SUBLINEA_M001 = SLM.SUBLINEAMATERIAL_ID
        -- LEFT JOIN CON.DIM_MAT_RESERVADO_REV SLM
        -- ON SL.A_SUBLINEA_M001 = SLM.RESERVADO_ID          
        LEFT JOIN CON.DIM_MAT_MARCA_ADH MAR
        ON M.MARCA_ADH_ID = MAR.MARCA_ADH_ID
        LEFT JOIN CON.DIM_MAT_MARCAGRUPO_ADH MG
        ON M.MARCAGRUPO_ADH_ID = MG.MARCAGRUPO_ADH_ID
        LEFT JOIN CON.DIM_MAT_NIVELPRECIO_ADH NP
        ON M.NIVELPRECIO_ADH_ID = NP.NIVELPRECIO_ADH_ID
        LEFT JOIN CON.DIM_MAT_ORIGENMATERIAL_ADH OM
        ON M.ORIGENMATERIAL_ADH_ID = OM.ORIGENMATERIAL_ADH_ID
        LEFT JOIN CON.DIM_MAT_ORIGENPAIS_ADH OP
        ON M.ORIGENPAIS_ADH_ID = OP.ORIGENPAIS_ADH_ID
        LEFT JOIN CON.DIM_MAT_PRESENTACION_ADH PR
        ON M.PRESENTACION_ID = PR.PRESENTACION_ID
        LEFT JOIN CON.DIM_MAT_FAMILIA_ADH FM -- FAMILIA_ADH antes era SUBLINEAMATERIAL_ADH
        ON M.FAMILIA_ADH_ID = FM.FAMILIA_ADH_ID
        LEFT JOIN CON.DIM_MAT_TIPOEMPAQUE_ADH TE
        ON M.TIPOEMPAQUE_ID = TE.TIPOEMPAQUE_ID
        ;
    ---------------------------------------------------------------------------------
    -- STEP 5: CLONADO PARA PRODUCCIÓN DIM_MAT_ADH_PP. Se necesita para producir

    ---------------------------------------------------------------------------------

    CREATE OR ALTER TABLE MIRRORING.DIM_MAT_ADH_PP CLONE MIRRORING.DIM_MAT_MATERIAL_ADH;
    create or replace stream LAMOSALAKE_DEV.MIRRORING.STREAM_DIM_MAT_ADH_PP ON table  MIRRORING.DIM_MAT_ADH_PP;
    ---------------------------------------------------------------------------------
    -- STEP 6: FINALIZACIÓN

    ---------------------------------------------------------------------------------
    RETURN CONCAT('Complete - Filas insertadas: ', ROWS_INSERTED);

END;
$$;
 
call CON.SP_CON_DIM_MAT_MATERIAL();