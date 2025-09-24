CREATE OR REPLACE ROW ACCESS POLICY RAP_SAP_DIM_CENTRO
AS (CENTRO_ID VARCHAR (4))

RETURNS BOOLEAN ->

CASE 

-- Caso 1: Usuario con rol SAP_ROLE tiene acceso total
    WHEN EXISTS(
        SELECT 1
        FROM SEGURIDAD.SEG_USUARIO_ROLE
        WHERE USER_NM = CURRENT_USER
        AND ROLE_NM = 'SAP_ROLE'
    ) THEN TRUE

-- Caso 2: Si el usuario no tiene nungún rol asignado -> Denegar acceso
    WHEN NOT EXISTS(
        SELECT 1
        FROM SEGURIDAD.SEG_USUARIO_ROLE
        WHERE USER_NM = CURRENT_USER
    ) THEN FALSE

-- Caso 3: Usuario tiene valores específicos en seguridad de usuario -> Prioridad sobre "ALL"
    WHEN EXISTS(
        SELECT 1
        FROM SEGURIDAD.SEG_USUARIO_DIMENSION
        WHERE USER_NM = CURRENT_USER
        AND DIM = 'CENTRO_ID'
    ) AND
    (
            -- Permitir acceso si hay coincidencia con valores específicos.
            CENTRO_ID IN (
                SELECT UD.VALOR
                FROM SEGURIDAD.SEG_USUARIO_DIMENSION UD
                JOIN SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION RD
                ON UD.VALOR = RD.VALOR_ROL
                WHERE UD.USER_NM = CURRENT_USER
                AND UD.DIM = 'CENTRO_ID'
                AND RD.USUARIO = CURRENT_USER
                AND RD.DIM_ROL = 'CENTRO_ID'
            )
            -- O si el usuario tiene "ALL", permetir acceso a todos sus valores individuales en USER_DIMENSION
            OR 
            
                CENTRO_ID IN (
                    SELECT UD.VALOR
                    FROM SEGURIDAD.SEG_USUARIO_DIMENSION UD 
                    WHERE UD.USER_NM = CURRENT_USER
                    AND UD.DIM = 'CENTRO_ID'
                ) 
                AND EXISTS(
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
    WHEN NOT EXISTS(
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
    WHEN NOT EXISTS(
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