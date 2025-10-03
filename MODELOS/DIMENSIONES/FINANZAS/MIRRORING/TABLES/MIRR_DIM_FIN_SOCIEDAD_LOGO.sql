CREATE OR REPLACE TABLE MIRRORING.DIM_FIN_SOCIEDAD_LOGO (

    SOCIEDAD_ID VARCHAR,

    LOGO        VARCHAR

) AS

WITH base AS (

    SELECT DISTINCT

        SOCIEDAD_ID,

        CASE

            WHEN SOCIEDAD_ID = 'A101' THEN 'https://grupolamosa.sharepoint.com/:i:/r/sites/MEX-SER-FIN-ProyectoBIS4H2/Shared%20Documents/01%20-%20Adhesivos/04%20-%20ADH%20-%20Finanzas/04%20-%20ADH%20-%20Finanzas%20-%20Cartera/04%20-%20Preparaci%C3%B3n%20Final%20-%20Cutover/LogosLamosa/A101%20-%20CREST.png?csf=1&web=1&e=X9lW21'
            WHEN SOCIEDAD_ID = 'A102' THEN 'https://grupolamosa.sharepoint.com/:i:/r/sites/MEX-SER-FIN-ProyectoBIS4H2/Shared%20Documents/01%20-%20Adhesivos/04%20-%20ADH%20-%20Finanzas/04%20-%20ADH%20-%20Finanzas%20-%20Cartera/04%20-%20Preparaci%C3%B3n%20Final%20-%20Cutover/LogosLamosa/A102%20-%20PERDURA.png?csf=1&web=1&e=7CX4jt'
            WHEN SOCIEDAD_ID = 'A103' THEN 'https://grupolamosa.sharepoint.com/:i:/r/sites/MEX-SER-FIN-ProyectoBIS4H2/Shared%20Documents/01%20-%20Adhesivos/04%20-%20ADH%20-%20Finanzas/04%20-%20ADH%20-%20Finanzas%20-%20Cartera/04%20-%20Preparaci%C3%B3n%20Final%20-%20Cutover/LogosLamosa/A103%20-%20NIASA.jpg?csf=1&web=1&e=YwgnW6'
            ELSE 'https://grupolamosa.sharepoint.com/:i:/r/sites/MEX-SER-FIN-ProyectoBIS4H2/Shared%20Documents/01%20-%20Adhesivos/04%20-%20ADH%20-%20Finanzas/04%20-%20ADH%20-%20Finanzas%20-%20Cartera/04%20-%20Preparaci%C3%B3n%20Final%20-%20Cutover/LogosLamosa/SOCIEDADES%20EXTRA.png?csf=1&web=1&e=8bNBML'

        END AS logo_sharepoint

    FROM MIRRORING.FCT_FIN_ADH_CARTERA

)

SELECT

    SOCIEDAD_ID,

    /* quitamos lo que venga después de "?" y le pegamos "?raw=1" */

    REGEXP_REPLACE(logo_sharepoint, '\\?.*$', '') || '?download=1' AS LOGO

FROM base;


CREATE OR REPLACE STREAM MIRRORING.STREAM_DIM_FIN_SOCIEDAD_LOGO
ON TABLE MIRRORING.DIM_FIN_SOCIEDAD_LOGO;