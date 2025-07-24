CREATE OR REPLACE ROW ACCESS POLICY CON.RAP_SAP_DIM_CENTRO_ID
AS (CENTRO_ID varchar(20))
RETURNS BOOLEAN ->
CASE 
    -- Caso 1: Usuario con rol SAP_ROLE tiene acceso total
    WHEN (SELECT SEGURIDAD.FN_VALIDA_SAP_ROLE())=TRUE THEN TRUE

    -- Caso 2: Si el usuario no tiene ningún rol asignado → Denegar acceso
    WHEN NOT EXISTS (
        SELECT 1 
        FROM SEGURIDAD.SEG_USUARIO_ROLE 
        WHERE USER_NM = CURRENT_USER
			) THEN FALSE

    -- Caso 3: Usuario tiene valores específicos en seguridad de usuario → Prioridad sobre "ALL"
    WHEN (
        (SELECT SEGURIDAD.FN_VALIDA_USUARIO_DIM('CENTRO_ID')) =TRUE
		AND 
        -- Permitir acceso si hay coincidencia con valores específicos
       (SELECT SEGURIDAD.FN_LISTA_DIM_ESPECIFICAS (CENTRO_ID, 'CENTRO_ID'))=TRUE
        -- O si el usuario tiene "ALL", permitir acceso a todos sus valores individuales en USER_DIMENSION
        OR CENTRO_ID IN (
                SELECT U.VALOR 
                FROM SEGURIDAD.SEG_USUARIO_DIMENSION U
                WHERE U.USER_NM = CURRENT_USER 
                AND U.DIM = 'CENTRO_ID'
			)
        AND EXISTS (
                SELECT 1 
                FROM SEGURIDAD.SEG_ROLE_DIMENSION 
                WHERE ROLE_NM IN (
                    SELECT ROLE_NM 
                    FROM SEGURIDAD.SEG_USUARIO_ROLE 
                    WHERE USER_NM = CURRENT_USER
                    ) 
                AND DIM = 'CENTRO_ID'
                AND VALOR = 'ALL'
            )
    ) THEN TRUE
    -- Caso 4: Si no tiene valores específicos, pero el rol tiene valores, usar los valores del rol
    WHEN NOT EXISTS (
        SELECT 1 
        FROM SEGURIDAD.SEG_USUARIO_DIMENSION 
        WHERE USER_NM = CURRENT_USER 
        AND DIM = 'CENTRO_ID'
    ) 
    AND CENTRO_ID IN (
        SELECT VALOR 
        FROM SEGURIDAD.SEG_ROLE_DIMENSION 
        WHERE ROLE_NM IN (
            SELECT ROLE_NM 
            FROM SEGURIDAD.SEG_USUARIO_ROLE 
            WHERE USER_NM = CURRENT_USER
        ) 
        AND DIM = 'CENTRO_ID'
    ) THEN TRUE
    -- Caso 5: Si no tiene valores específicos y el rol tiene "ALL", permitir acceso total
    WHEN NOT EXISTS (
        SELECT 1 
        FROM SEGURIDAD.SEG_USUARIO_DIMENSION 
        WHERE USER_NM = CURRENT_USER 
        AND DIM = 'CENTRO_ID'
    ) 
    AND EXISTS (
        SELECT 1 
        FROM SEGURIDAD.SEG_ROLE_DIMENSION 
        WHERE ROLE_NM IN (
            SELECT ROLE_NM 
            FROM SEGURIDAD.SEG_USUARIO_ROLE 
            WHERE USER_NM = CURRENT_USER
        ) 
        AND DIM = 'CENTRO_ID' 
        AND VALOR = 'ALL'
    ) THEN TRUE

    -- Caso 6: Denegar acceso en cualquier otro caso
    ELSE FALSE

END;

 