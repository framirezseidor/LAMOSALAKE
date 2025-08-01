CREATE OR REPLACE VIEW CON.VW_DIM_MAT_MATERIAL_ADH AS
SELECT
    M.MATERIAL_ID,
    M.MATERIAL_TEXT,
    M.TIPOMATERIAL_ID,
    TM.TIPOMATERIAL_TEXT,
    M.GRUPOARTICULOS_ID,
    GRUPOMATERIALES.GRUPOMATERIALES_TEXT AS GRUPOARTICULOS_TEXT,-- NO EXISTE GRUPOARTICULOS_TEXT, SE ESTA USANDO GRUPOMATERIALES_TEXT EN LUGAR DE GRUPOARTICULOS_TEXT
    M.STATUSMAT_TODOSCENTROS_ID,
    STATUSMATERIAL.STATUSMATERIAL_TEXT AS STATUSMAT_TODOSCENTROS_TEXT, -- NO EXISTE STATUSMAT_TODOSCENTROS_TEXT, SE ESTA USANDO STATUSMATERIAL_TEXT EN LUGAR DE STATUSMAT_TODOSCENTROS_TEXT
    M.COLOR_ADH_ID,
    COLOR_ADH.COLOR_ADH_TEXT,
    M.COLORGRUPO_ADH_ID,
    COLORGRUPO_ADH.COLORGRUPO_ADH_TEXT,
    M.FAMILIA_ADH_ID,
    FAMILIA_ADH.FAMILIA_ADH_TEXT,
    M.MARCA_ADH_ID,
    MARCA_ADH.MARCA_ADH_TEXT,
    M.MARCAGRUPO_ADH_ID,
    MARCAGRUPO_ADH.MARCAGRUPO_ADH_TEXT,
    M.NIVELPRECIO_ADH_ID,
    NIVELPRECIO_ADH.NIVELPRECIO_ADH_TEXT,
    M.ORIGENMATERIAL_ADH_ID,
    ORIGENMATERIAL_ADH.ORIGENMATERIAL_ADH_TEXT,
    M.ORIGENPAIS_ADH_ID,
    ORIGENPAIS_ADH.ORIGENPAIS_ADH_TEXT,
    M.PRESENTACION_ID,
    PRESENTACION_ADH.PRESENTACION_TEXT,
    M.TIPOEMPAQUE_ID,
    TIPOEMPAQUE_ADH.TIPOEMPAQUE_TEXT,
    M.LINEAMATERIAL_ADH_ID,
    LINEAMATERIAL_ADH.LINEAMATERIAL_TEXT AS LINEAMATERIAL_ADH_TEXT, -- NO EXISTE LINEAMATERIAL_ADH_TEXT, SE ESTA USANDO LINEAMATERIAL_TEXT EN LUGAR DE LINEAMATERIAL_ADH_TEXT
    --M.SUBLINEAMATERIAL_ADH_ID,
    --SUBLINEAMATERIAL_ADH.SUBLINEAMATERIAL_ADH_TEXT,
    M.SISORIGEN_ID,
    M.FECHA_CARGA,
    M.ZONA_HORARIA,
    M.MANDANTE
FROM  CON.DIM_MAT_MATERIAL AS  M
LEFT JOIN CON.DIM_MAT_TIPOMATERIAL AS TM
    ON M.TIPOMATERIAL_ID = TM.TIPOMATERIAL_ID
LEFT JOIN CON.DIM_MAT_GRUPOMATERIALES AS GRUPOMATERIALES 
    ON M.GRUPOARTICULOS_ID =  GRUPOMATERIALES.GRUPOMATERIALES_ID -- NO EXISTE GRUPOARTICULOS_ID EN DIM_MAT_GRUPOMATERIALES, Se esta usando GRUPOMATERIALES_ID en lugar de GRUPOARTICULOS_ID
LEFT JOIN CON.DIM_MAT_STATUSMATERIAL AS STATUSMATERIAL 
    ON M.STATUSMAT_TODOSCENTROS_ID = STATUSMATERIAL.STATUSMATERIAL_ID
LEFT JOIN CON.DIM_MAT_COLOR_ADH AS COLOR_ADH
    ON M.COLOR_ADH_ID = COLOR_ADH.COLOR_ADH_ID
LEFT JOIN CON.DIM_MAT_COLORGRUPO_ADH AS COLORGRUPO_ADH
    ON M.COLORGRUPO_ADH_ID = COLORGRUPO_ADH.COLORGRUPO_ADH_ID
LEFT JOIN CON.DIM_MAT_FAMILIA_ADH AS FAMILIA_ADH -- ANTIGUAMENTE ERA SUBLINEA MATERIAL ADH
    ON M.FAMILIA_ADH_ID = FAMILIA_ADH.FAMILIA_ADH_ID
LEFT JOIN CON.DIM_MAT_MARCA_ADH AS MARCA_ADH
    ON M.MARCA_ADH_ID = MARCA_ADH.MARCA_ADH_ID
LEFT JOIN CON.DIM_MAT_MARCAGRUPO_ADH AS MARCAGRUPO_ADH
    ON M.MARCAGRUPO_ADH_ID = MARCAGRUPO_ADH.MARCAGRUPO_ADH_ID
LEFT JOIN CON.DIM_MAT_NIVELPRECIO_ADH AS NIVELPRECIO_ADH
    ON M.NIVELPRECIO_ADH_ID = NIVELPRECIO_ADH.NIVELPRECIO_ADH_ID
LEFT JOIN CON.DIM_MAT_ORIGENMATERIAL_ADH AS ORIGENMATERIAL_ADH
    ON M.ORIGENMATERIAL_ADH_ID = ORIGENMATERIAL_ADH.ORIGENMATERIAL_ADH_ID
LEFT JOIN CON.DIM_MAT_ORIGENPAIS_ADH AS ORIGENPAIS_ADH
    ON M.ORIGENPAIS_ADH_ID = ORIGENPAIS_ADH.ORIGENPAIS_ADH_ID
LEFT JOIN CON.DIM_MAT_PRESENTACION_ADH AS PRESENTACION_ADH
    ON M.PRESENTACION_ID = PRESENTACION_ADH.PRESENTACION_ID
LEFT JOIN CON.DIM_MAT_TIPOEMPAQUE_ADH AS TIPOEMPAQUE_ADH
    ON M.TIPOEMPAQUE_ID = TIPOEMPAQUE_ADH.TIPOEMPAQUE_ID
LEFT JOIN CON.DIM_MAT_LINEAMATERIAL_ADH AS LINEAMATERIAL_ADH
    ON M.LINEAMATERIAL_ADH_ID = LINEAMATERIAL_ADH.LINEAMATERIAL_ID; -- NO EXISTE LINEAMATERIAL_ADH_ID, SE ESTA USANDO LINEAMATERIAL_ID EN LUGAR DE LINEAMATERIAL_ADH_ID
--LEFT JOIN CON.DIM_MAT_SUBLINEAMATERIAL_ADH AS SUBLINEAMATERIAL_ADH--ESTAMOS AQUI
   -- ON M.SU = SUBLINEAMATERIAL_ADH ------- NO EXISTE ESTE CAMPO EN LA DIM MATERIAL