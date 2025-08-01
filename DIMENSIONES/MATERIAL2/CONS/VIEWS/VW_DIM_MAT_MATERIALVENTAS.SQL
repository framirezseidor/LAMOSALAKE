CREATE OR REPLACE VIEW CON.VW_DIM_MAT_MATERIALVENTAS AS
SELECT
    MVENTAS.MATERIALVENTAS_ID,
    MVENTAS.MATERIALVENTAS_TEXT,
    MVENTAS.MATERIAL_ID,
    M.MATERIAL_TEXT,
    MVENTAS.ORGVENTAS_ID,
    ORG_VENTAS.ORGVENTAS_TEXT,
    MVENTAS.CANALDISTRIB_ID,
    CANAL_DISTRIBUCION.CANALDISTRIB_TEXT,
    MVENTAS.GRUPOIMPUTACION_ID,
    GRUPO_IMPUTACION.GRUPOIMPUTACION_TEXT,
    MVENTAS.GRUPOMATERIALES_ID,
    GRUPO_MAT.GRUPOMATERIALES_TEXT, 
    MVENTAS.GRUPOMATERIALES1_ID,
    GRUPO_MAT1.GRUPOMATERIALES1_TEXT, 
    MVENTAS.GRUPOMATERIALES2_ID,
    GRUPO_MAT2.GRUPOMATERIALES2_TEXT, 
    MVENTAS.GRUPOMATERIALES3_ID,
    GRUPO_MAT3.GRUPOMATERIALES3_TEXT,  
    MVENTAS.GRUPOMATERIALES4_ID,
    GRUPO_MAT4.GRUPOMATERIALES4_TEXT, 
    MVENTAS.GRUPOMATERIALES5_ID,
    GRUPO_MAT5.GRUPOMATERIALES5_TEXT, 
    MVENTAS.STATUSMAT_CANDIS_ID,
    STATUS_MAT.STATUSMATERIAL_TEXT,
    MVENTAS.SISORIGEN_ID,
    MVENTAS.FECHA_CARGA,
    MVENTAS.ZONA_HORARIA,
    MVENTAS.MANDANTE
FROM  CON.DIM_MAT_MATERIALVENTAS AS  MVENTAS
LEFT JOIN CON.DIM_MAT_MATERIAL AS M
    ON MVENTAS.MATERIAL_ID = M.MATERIAL_ID 
LEFT JOIN CON.DIM_ORG_ORGANIZACIONVENTAS AS ORG_VENTAS 
    ON MVENTAS.ORGVENTAS_ID = ORG_VENTAS.ORGVENTAS_ID 
LEFT JOIN CON.DIM_ORG_CANALDISTRIBUCION AS CANAL_DISTRIBUCION
    ON MVENTAS.CANALDISTRIB_ID = CANAL_DISTRIBUCION.CANALDISTRIB_ID
LEFT JOIN CON.DIM_MAT_GRUPOIMPUTACION AS GRUPO_IMPUTACION 
    ON MVENTAS.GRUPOIMPUTACION_ID = GRUPO_IMPUTACION.GRUPOIMPUTACION_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES AS GRUPO_MAT 
    ON MVENTAS.GRUPOMATERIALES_ID = GRUPO_MAT.GRUPOMATERIALES_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES1 AS GRUPO_MAT1  
    ON MVENTAS.GRUPOMATERIALES1_ID = GRUPO_MAT1.GRUPOMATERIALES1_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES2 AS GRUPO_MAT2  
    ON MVENTAS.GRUPOMATERIALES2_ID = GRUPO_MAT2.GRUPOMATERIALES2_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES3 AS GRUPO_MAT3  
    ON MVENTAS.GRUPOMATERIALES3_ID = GRUPO_MAT3.GRUPOMATERIALES3_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES4 AS GRUPO_MAT4  
    ON MVENTAS.GRUPOMATERIALES4_ID = GRUPO_MAT4.GRUPOMATERIALES4_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES5 AS GRUPO_MAT5  
    ON MVENTAS.GRUPOMATERIALES5_ID = GRUPO_MAT5.GRUPOMATERIALES5_ID
LEFT JOIN CON.DIM_MAT_STATUSMATERIAL AS STATUS_MAT 
    ON MVENTAS.STATUSMAT_CANDIS_ID = STATUS_MAT.STATUSMATERIAL_ID
