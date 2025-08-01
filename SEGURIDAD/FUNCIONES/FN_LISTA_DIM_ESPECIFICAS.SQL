CREATE OR REPLACE FUNCTION SEGURIDAD.FN_LISTA_DIM_ESPECIFICAS (VALOR VARCHAR(100), NOMBRE_DIM VARCHAR(50))
RETURNS BOOLEAN
LANGUAGE SQL
AS 
$$
    SELECT EXISTS(
            SELECT U.VALOR 
            FROM SEGURIDAD.SEG_USUARIO_DIMENSION U
            JOIN SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION R
            ON U.VALOR = R.VALOR_ROL
            WHERE U.USER_NM = CURRENT_USER 
            AND U.DIM = NOMBRE_DIM
            AND R.USUARIO = CURRENT_USER 
            AND R.DIM_ROL = NOMBRE_DIM
            AND U.VALOR = VALOR)
$$;

