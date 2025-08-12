CREATE OR REPLACE PROCEDURE RAW.SP_RAW_BORRADO_PRERAW(
    EXTRACTOR       VARCHAR(50),
    NEGOCIO         VARCHAR(30)
)
RETURNS VARCHAR(200)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/*
---------------------------------------------------------------------------------
 Versión:            1.0
 Fecha de creación:  2025-05-12
 Creador:            Fidel Ramírez
 Descripción:        PRE BORRADO RAW
---------------------------------------------------------------------------------
*/

DECLARE
    -- VARIABLES DE CONTROL
    F_INICIO        TIMESTAMP_NTZ(9);
    F_FIN           TIMESTAMP_NTZ(9);
    SOCIEDAD_INICIO   VARCHAR(20);
    SOCIEDAD_FIN      VARCHAR(20);
    PREFIJO         VARCHAR(5);
    TABLA           VARCHAR(500);
    TEXTOSQL         VARCHAR(1000);


BEGIN
    ---------------------------------------------------------------------------------
    -- STEP 0: VALORES DE SOCIEDADES
    ---------------------------------------------------------------------------------

    SELECT RAW.FN_BUSCA_PARAMETRO( :EXTRACTOR , :NEGOCIO , 'VKORG') INTO :SOCIEDAD_INICIO ;

    SOCIEDAD_FIN := SPLIT_PART(SOCIEDAD_INICIO,'-',2);
    SOCIEDAD_INICIO := SPLIT_PART(SOCIEDAD_INICIO,'-',1);  

    SELECT RAW.FN_BUSCA_PARAMETRO( 'EXTRACTOR_TX' , 'GENERAL' , 'PREFIJO') INTO :PREFIJO ;
    PREFIJO := SPLIT_PART(PREFIJO,'-',1);    
    TABLA := 'RAW.' || :PREFIJO || 'MOF_ZBWSD_CUADERNO_FINANCIERO';

    TEXTOSQL := CONCAT('DELETE FROM ', :TABLA, ' WHERE VKORG BETWEEN ''', SOCIEDAD_INICIO,''' AND ''', SOCIEDAD_FIN,''' ') ;

    EXECUTE IMMEDIATE (:TEXTOSQL);
END;

$$