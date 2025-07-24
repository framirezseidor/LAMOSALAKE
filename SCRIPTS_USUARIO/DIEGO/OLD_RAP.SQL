CREATE OR REPLACE ROW ACCESS POLICY RAP_SAP_DIM_SOCIEDAD

AS (SOCIEDAD_ID VARCHAR(4))

RETURNS BOOLEAN ->

CASE

    -- Caso 1: Usuario con rol SAP_ROLE tiene acceso total

    WHEN EXISTS (

        SELECT 1

        FROM SEGURIDAD.CON_SEG_USUARIO_ROLE

        WHERE USER_NM = CURRENT_USER

        AND ROLE_NM = 'SAP_ROLE'

                                               ) THEN TRUE

    -- Caso 2: Si el usuario no tiene ningún rol asignado → Denegar acceso

    WHEN NOT EXISTS (

        SELECT 1

        FROM SEGURIDAD.CON_SEG_USUARIO_ROLE

        WHERE USER_NM = CURRENT_USER

                                               ) THEN FALSE

    -- Caso 3: Usuario tiene valores específicos en seguridad de usuario → Prioridad sobre "ALL"

    WHEN EXISTS (

        SELECT 1

        FROM SEGURIDAD.CON_SEG_USUARIO_DIMENSION

        WHERE USER_NM = CURRENT_USER

        AND DIM = 'SOCIEDAD_ID'

                               )

                               AND (

        -- Permitir acceso si hay coincidencia con valores específicos

        SOCIEDAD_ID IN (

            SELECT U.VALOR

            FROM SEGURIDAD.CON_SEG_USUARIO_DIMENSION U

            JOIN SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION R

            ON U.VALOR = R.VALOR_ROL

            WHERE U.USER_NM = CURRENT_USER

            AND U.DIM = 'SOCIEDAD_ID'

            AND R.USUARIO = CURRENT_USER

            AND R.DIM_ROL = 'SOCIEDAD_ID'

                                               )

        -- O si el usuario tiene "ALL", permitir acceso a todos sus valores individuales en USER_DIMENSION

        OR SOCIEDAD_ID IN (

            SELECT U.VALOR

            FROM SEGURIDAD.CON_SEG_USUARIO_DIMENSION U

            WHERE U.USER_NM = CURRENT_USER

            AND U.DIM = 'SOCIEDAD_ID'

                                               )

        AND EXISTS (

            SELECT 1

            FROM SEGURIDAD.CON_SEG_ROLE_DIMENSION

            WHERE ROLE_NM IN (

                SELECT ROLE_NM

                FROM SEGURIDAD.CON_SEG_USUARIO_ROLE

                WHERE USER_NM = CURRENT_USER

            )

            AND DIM = 'SOCIEDAD_ID'

            AND VALOR = 'ALL'

        )

    ) THEN TRUE

    -- Caso 4: Si no tiene valores específicos, pero el rol tiene valores, usar los valores del rol

    WHEN NOT EXISTS (

        SELECT 1

        FROM SEGURIDAD.CON_SEG_USUARIO_DIMENSION

        WHERE USER_NM = CURRENT_USER

        AND DIM = 'SOCIEDAD_ID'

    )

    AND SOCIEDAD_ID IN (

        SELECT VALOR

        FROM SEGURIDAD.CON_SEG_ROLE_DIMENSION

        WHERE ROLE_NM IN (

            SELECT ROLE_NM

            FROM SEGURIDAD.CON_SEG_USUARIO_ROLE

            WHERE USER_NM = CURRENT_USER

        )

        AND DIM = 'SOCIEDAD_ID'

    ) THEN TRUE

    -- Caso 5: Si no tiene valores específicos y el rol tiene "ALL", permitir acceso total

    WHEN NOT EXISTS (

        SELECT 1

        FROM SEGURIDAD.CON_SEG_USUARIO_DIMENSION

        WHERE USER_NM = CURRENT_USER

        AND DIM = 'SOCIEDAD_ID'

    )

    AND EXISTS (

        SELECT 1

        FROM SEGURIDAD.CON_SEG_ROLE_DIMENSION

        WHERE ROLE_NM IN (

            SELECT ROLE_NM

            FROM SEGURIDAD.CON_SEG_USUARIO_ROLE

            WHERE USER_NM = CURRENT_USER

        )

        AND DIM = 'SOCIEDAD_ID'

        AND VALOR = 'ALL'

    ) THEN TRUE

 

    -- Caso 6: Denegar acceso en cualquier otro caso

    ELSE FALSE

 

END;