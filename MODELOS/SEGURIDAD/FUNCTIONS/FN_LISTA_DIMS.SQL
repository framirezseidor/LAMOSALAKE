CREATE OR REPLACE FUNCTION SEGURIDAD.FN_LISTA_DIMS()
  RETURNS INTEGER
  LANGUAGE SQL
  AS
  $$
     CASE 
            WHEN (
              SELECT COUNT(DISTINCT DIM_ROL)
              FROM SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION 
              WHERE 
                USUARIO = CURRENT_USER
                AND DIM_ROL IN ('COORDINADORDEST_ID', 'ASESORDEST_ID')
            ) = 2
            THEN 1
           WHEN (
              SELECT COUNT(DISTINCT DIM_ROL)
              FROM SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION 
              WHERE 
                USUARIO = CURRENT_USER
                AND DIM_ROL = 'COORDINADORDEST_ID'
                AND DIM_ROL != 'ASESORDEST_ID' 
            ) = 1
            THEN 2            
           WHEN (
              SELECT COUNT(DISTINCT DIM_ROL)
              FROM SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION 
              WHERE 
                USUARIO = CURRENT_USER
                AND DIM_ROL != 'COORDINADORDEST_ID'
                AND DIM_ROL = 'ASESORDEST_ID' 
            ) = 1
            THEN 3                       
            WHEN (
              SELECT COUNT(DISTINCT DIM_ROL)
              FROM SEGURIDAD.VW_CON_SEG_USUARIO_ROLE_DIMENSION 
              WHERE 
                USUARIO = CURRENT_USER
                AND DIM_ROL IN ('COORDINADORDEST_ID', 'ASESORDEST_ID')
            ) = 0
            THEN 4            
            ELSE 0
          END
  $$;