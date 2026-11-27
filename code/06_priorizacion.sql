CREATE OR REPLACE TABLE `cdc-big-data-analitica-2026.epi_analytics.priorizacion_regiones` AS
WITH parametros AS (
  SELECT
    0.50 AS peso_casos,
    0.30 AS peso_oferta,
    0.20 AS peso_letalidad
),

-- Las dos enfermedades del proyecto. El patron tolera tildes y mayusculas.
grupos AS (
  SELECT 'DENGUE' AS grupo, 'Dengue' AS grupo_nombre, '%DENGUE%' AS patron
  UNION ALL
  SELECT 'CARRION', 'Enfermedad de Carrión', '%CARRI%'
),

-- Lista fija de los 25 departamentos. Garantiza 25 filas por enfermedad
-- aunque una fuente no tenga datos para alguna region.
departamentos AS (
  SELECT dep, region, region_geo
  FROM UNNEST([
    STRUCT('01' AS dep, 'Amazonas' AS region, 'Amazonas' AS region_geo),
    STRUCT('02' AS dep, 'Áncash' AS region, 'Ancash' AS region_geo),
    STRUCT('03' AS dep, 'Apurímac' AS region, 'Apurimac' AS region_geo),
    STRUCT('04' AS dep, 'Arequipa' AS region, 'Arequipa' AS region_geo),
    STRUCT('05' AS dep, 'Ayacucho' AS region, 'Ayacucho' AS region_geo),
    STRUCT('06' AS dep, 'Cajamarca' AS region, 'Cajamarca' AS region_geo),
    STRUCT('07' AS dep, 'Callao' AS region, 'Callao' AS region_geo),
    STRUCT('08' AS dep, 'Cusco' AS region, 'Cusco' AS region_geo),
    STRUCT('09' AS dep, 'Huancavelica' AS region, 'Huancavelica' AS region_geo),
    STRUCT('10' AS dep, 'Huánuco' AS region, 'Huanuco' AS region_geo),
    STRUCT('11' AS dep, 'Ica' AS region, 'Ica' AS region_geo),
    STRUCT('12' AS dep, 'Junín' AS region, 'Junin' AS region_geo),
    STRUCT('13' AS dep, 'La Libertad' AS region, 'La Libertad' AS region_geo),
    STRUCT('14' AS dep, 'Lambayeque' AS region, 'Lambayeque' AS region_geo),
    STRUCT('15' AS dep, 'Lima' AS region, 'Lima' AS region_geo),
    STRUCT('16' AS dep, 'Loreto' AS region, 'Loreto' AS region_geo),
    STRUCT('17' AS dep, 'Madre de Dios' AS region, 'Madre de Dios' AS region_geo),
    STRUCT('18' AS dep, 'Moquegua' AS region, 'Moquegua' AS region_geo),
    STRUCT('19' AS dep, 'Pasco' AS region, 'Pasco' AS region_geo),
    STRUCT('20' AS dep, 'Piura' AS region, 'Piura' AS region_geo),
    STRUCT('21' AS dep, 'Puno' AS region, 'Puno' AS region_geo),
    STRUCT('22' AS dep, 'San Martín' AS region, 'San Martin' AS region_geo),
    STRUCT('23' AS dep, 'Tacna' AS region, 'Tacna' AS region_geo),
    STRUCT('24' AS dep, 'Tumbes' AS region, 'Tumbes' AS region_geo),
    STRUCT('25' AS dep, 'Ucayali' AS region, 'Ucayali' AS region_geo)
  ])
),

-- Ano de prueba del modelo. Es el mismo para las dos enfermedades.
corte AS (
  SELECT UPPER(grupo) AS grupo, MAX(ano) AS ano
  FROM `cdc-big-data-analitica-2026.epi_analytics.predicciones`
  WHERE horizonte = 1
    AND UPPER(grupo) IN ('DENGUE', 'CARRION')
  GROUP BY grupo
),

-- Acumulado del ano por region. semanas_con_dato deja ver cuantas semanas
-- aportaron y evita leer un acumulado parcial como si fuera el ano completo.
proyectado AS (
  SELECT
    UPPER(p.grupo) AS grupo,
    SUBSTR(LPAD(CAST(p.ubigeo AS STRING), 6, '0'), 1, 2) AS dep,
    SUM(p.prediccion) AS casos_proyectados,
    COUNT(DISTINCT p.semana) AS semanas_con_dato
  FROM `cdc-big-data-analitica-2026.epi_analytics.predicciones` p
  JOIN corte c
    ON UPPER(p.grupo) = c.grupo
   AND p.ano = c.ano
  WHERE p.horizonte = 1
    AND UPPER(p.grupo) IN ('DENGUE', 'CARRION')
  GROUP BY grupo, dep
),

-- Casos observados en el ano anterior, como contexto de crecimiento.
observado AS (
  SELECT
    UPPER(e.grupo) AS grupo,
    SUBSTR(LPAD(CAST(e.ubigeo AS STRING), 6, '0'), 1, 2) AS dep,
    SUM(e.casos) AS casos_ano_prev
  FROM `cdc-big-data-analitica-2026.epi_analytics.epi_clean` e
  JOIN corte c
    ON UPPER(e.grupo) = c.grupo
   AND e.ano = c.ano - 1
  WHERE UPPER(e.grupo) IN ('DENGUE', 'CARRION')
  GROUP BY grupo, dep
),

-- ipress_dim ya trae el conteo por ubigeo. Es el mismo para las dos enfermedades.
oferta AS (
  SELECT
    SUBSTR(LPAD(CAST(ubigeo AS STRING), 6, '0'), 1, 2) AS dep,
    SUM(n_ipress) AS ipress
  FROM `cdc-big-data-analitica-2026.epi_analytics.ipress_dim`
  GROUP BY dep
),

-- Letalidad del ultimo ano publicado, por enfermedad. La tabla no tiene
-- columna de departamento, asi que el valor es nacional.
boletines_max AS (
  SELECT g.grupo, MAX(b.anio) AS anio
  FROM grupos g
  JOIN `cdc-big-data-analitica-2026.epi_analytics.boletines_master` b
    ON UPPER(b.enfermedad) LIKE g.patron
  GROUP BY g.grupo
),

boletines AS (
  SELECT g.grupo, MAX(b.letalidad) AS letalidad
  FROM grupos g
  JOIN boletines_max m ON m.grupo = g.grupo
  JOIN `cdc-big-data-analitica-2026.epi_analytics.boletines_master` b
    ON UPPER(b.enfermedad) LIKE g.patron
   AND b.anio = m.anio
  GROUP BY g.grupo
),

base AS (
  SELECT
    g.grupo,
    g.grupo_nombre,
    d.dep,
    d.region,
    d.region_geo,
    -- Se conserva NULL cuando la region no tiene ninguna prediccion en el ano.
    p.casos_proyectados AS casos_proyectados,
    COALESCE(p.casos_proyectados, 0) AS casos_index,
    COALESCE(p.semanas_con_dato, 0) AS semanas_con_dato,
    COALESCE(h.casos_ano_prev, 0) AS casos_ano_prev,
    COALESCE(o.ipress, 0) AS ipress,
    -- Sin IPRESS, todos los casos caen en un solo establecimiento.
    SAFE_DIVIDE(
      COALESCE(p.casos_proyectados, 0),
      GREATEST(COALESCE(o.ipress, 0), 1)
    ) AS casos_por_ipress,
    COALESCE(bl.letalidad, 0) AS letalidad,
    IF(p.casos_proyectados IS NULL,
       'Sin prediccion', 'Con prediccion') AS estado_dato,
    c.ano AS ano_corte
  FROM grupos g
  CROSS JOIN departamentos d
  LEFT JOIN oferta o USING (dep)
  LEFT JOIN proyectado p ON p.grupo = g.grupo AND p.dep = d.dep
  LEFT JOIN observado h ON h.grupo = g.grupo AND h.dep = d.dep
  LEFT JOIN boletines bl ON bl.grupo = g.grupo
  LEFT JOIN corte c ON c.grupo = g.grupo
),

normalizado AS (
  SELECT
    *,
    SAFE_DIVIDE(
      casos_index - MIN(casos_index) OVER (PARTITION BY grupo),
      NULLIF(MAX(casos_index) OVER (PARTITION BY grupo) -
             MIN(casos_index) OVER (PARTITION BY grupo), 0)
    ) AS n_casos,
    SAFE_DIVIDE(
      casos_por_ipress - MIN(casos_por_ipress) OVER (PARTITION BY grupo),
      NULLIF(MAX(casos_por_ipress) OVER (PARTITION BY grupo) -
             MIN(casos_por_ipress) OVER (PARTITION BY grupo), 0)
    ) AS n_oferta,
    SAFE_DIVIDE(
      letalidad - MIN(letalidad) OVER (PARTITION BY grupo),
      NULLIF(MAX(letalidad) OVER (PARTITION BY grupo) -
             MIN(letalidad) OVER (PARTITION BY grupo), 0)
    ) AS n_letalidad
  FROM base
),

puntaje AS (
  SELECT
    n.*,
    -- Si la letalidad no varia, su peso sale del denominador y el indice
    -- vuelve a la escala 0-100 sin cambiar el orden.
    SAFE_DIVIDE(
      p.peso_casos * COALESCE(n_casos, 0) +
      p.peso_oferta * COALESCE(n_oferta, 0) +
      p.peso_letalidad * COALESCE(n_letalidad, 0),
      p.peso_casos + p.peso_oferta +
        IF(n.n_letalidad IS NULL, 0, p.peso_letalidad)
    ) AS indice
  FROM normalizado n
  CROSS JOIN parametros p
)

SELECT
  ROW_NUMBER() OVER (
    PARTITION BY grupo
    ORDER BY IF(estado_dato = 'Sin prediccion', 1, 0),
             indice DESC, casos_index DESC, dep
  ) AS puesto,
  grupo,
  grupo_nombre,
  dep,
  region,
  region_geo,
  ROUND(casos_proyectados, 1) AS casos_proyectados,
  casos_ano_prev,
  semanas_con_dato,
  ipress,
  ROUND(casos_por_ipress, 2) AS casos_por_ipress,
  ROUND(letalidad, 4) AS letalidad,
  ROUND(indice * 100, 2) AS indice_prioridad,
  estado_dato,
  ano_corte,
  CURRENT_DATE() AS fecha_corte
FROM puntaje;


-- Vista del panel. Agrupa el ranking en niveles para el tablero y permite
-- filtrar por enfermedad.
--
-- El nivel es proporcional a las regiones con dato de cada enfermedad: 20%
-- superior Alta, siguiente 30% Media, resto Baja. Con un corte fijo, Carrión
-- solo tiene cinco regiones con dato y todas saldrían Alta, incluida una con
-- 0.1 casos. El criterio proporcional evita ese efecto.
CREATE OR REPLACE VIEW `cdc-big-data-analitica-2026.epi_analytics.vw_panel_priorizacion` AS
WITH ranking AS (
  SELECT
    *,
    COUNTIF(estado_dato = 'Con prediccion') OVER (PARTITION BY grupo) AS n_con_dato
  FROM `cdc-big-data-analitica-2026.epi_analytics.priorizacion_regiones`
)
SELECT
  grupo,
  grupo_nombre,
  puesto,
  dep,
  region,
  region_geo,
  casos_proyectados,
  casos_ano_prev,
  semanas_con_dato,
  ipress,
  casos_por_ipress,
  letalidad,
  indice_prioridad,
  estado_dato,
  ano_corte,
  fecha_corte,
  CASE
    WHEN estado_dato = 'Sin prediccion' THEN 'Sin dato'
    WHEN puesto <= CEIL(0.20 * n_con_dato) THEN 'Alta'
    WHEN puesto <= CEIL(0.50 * n_con_dato) THEN 'Media'
    ELSE 'Baja'
  END AS nivel_prioridad
FROM ranking;

-- Vista semanal 2024 para Dengue y Carrión (serie temporal para Looker Studio)
CREATE OR REPLACE VIEW `cdc-big-data-analitica-2026.epi_analytics.vw_semanal_2024` AS
WITH
-- Definimos los grupos del proyecto y sus patrones de búsqueda
grupos AS (
  SELECT 'DENGUE' AS grupo, 'Dengue' AS grupo_nombre, '%DENGUE%' AS patron
  UNION ALL
  SELECT 'CARRION', 'Enfermedad de Carrión', '%CARRI%'
),

-- Obtenemos la última semana con predicción (para 2024 es la 52)
corte AS (
  SELECT UPPER(grupo) AS grupo, MAX(ano) AS ano
  FROM `cdc-big-data-analitica-2026.epi_analytics.predicciones`
  WHERE horizonte = 1
    AND UPPER(grupo) IN ('DENGUE', 'CARRION')
  GROUP BY grupo
),

-- Sumamos las predicciones por semana y grupo para 2024
proyectado AS (
  SELECT
    UPPER(p.grupo) AS grupo,
    p.semana,
    SUM(p.prediccion) AS casos_proyectados
  FROM `cdc-big-data-analitica-2026.epi_analytics.predicciones` p
  JOIN corte c
    ON UPPER(p.grupo) = c.grupo
   AND p.ano = c.ano
  WHERE p.horizonte = 1
    AND UPPER(p.grupo) IN ('DENGUE', 'CARRION')
  GROUP BY UPPER(p.grupo), p.semana
),

-- Promedio de casos observados en la misma semana de años anteriores (2022-2023)
promedio_historico AS (
  SELECT
    UPPER(e.grupo) AS grupo,
    e.semana,
    AVG(e.casos) AS casos_ano_prev_promedio
  FROM `cdc-big-data-analitica-2026.epi_analytics.epi_clean` e
  WHERE UPPER(e.grupo) IN ('DENGUE', 'CARRION')
    AND e.ano IN (2022, 2023)
  GROUP BY UPPER(e.grupo), e.semana
)

-- Resultado final: semana, grupo, nombre, casos proyectados y promedio histórico
SELECT
  p.semana,
  p.grupo,
  g.grupo_nombre,
  p.casos_proyectados,
  COALESCE(ph.casos_ano_prev_promedio, 0) AS casos_ano_prev_promedio
FROM proyectado p
JOIN grupos g ON UPPER(p.grupo) = g.grupo
LEFT JOIN promedio_historico ph ON p.grupo = ph.grupo AND p.semana = ph.semana
ORDER BY p.grupo, p.semana;
