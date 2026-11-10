-- Consultas de ejemplo para Athena (workgroup "datalake-energia", base de datos "datalake_energia").
-- Filtrar siempre por "mes" hace que Athena lea solo esa particion: menos datos escaneados = menos coste.

-- 1. Precio medio, minimo y maximo de cada dia por indicador
SELECT
    fecha,
    indicador,
    ROUND(AVG(precio_eur_mwh), 2) AS precio_medio,
    MIN(precio_eur_mwh)           AS precio_min,
    MAX(precio_eur_mwh)           AS precio_max
FROM precios_electricidad
WHERE mes = '2026-11'
GROUP BY fecha, indicador
ORDER BY fecha, indicador;


-- 2. Las 3 horas mas baratas de cada dia para el PVPC (cuando conviene poner la lavadora)
SELECT fecha, hora, precio_eur_mwh
FROM (
    SELECT
        fecha,
        hora,
        precio_eur_mwh,
        ROW_NUMBER() OVER (PARTITION BY fecha ORDER BY precio_eur_mwh) AS posicion
    FROM precios_electricidad
    WHERE mes = '2026-11'
      AND indicador_id = '1001'  -- PVPC
)
WHERE posicion <= 3
ORDER BY fecha, posicion;


-- 3. Diferencia entre el PVPC (precio al consumidor) y el mercado mayorista, hora a hora
SELECT
    fecha_hora_utc,
    fecha,
    hora,
    MAX(CASE WHEN indicador_id = '1001' THEN precio_eur_mwh END) AS pvpc,
    MAX(CASE WHEN indicador_id = '600'  THEN precio_eur_mwh END) AS spot,
    MAX(CASE WHEN indicador_id = '1001' THEN precio_eur_mwh END)
      - MAX(CASE WHEN indicador_id = '600' THEN precio_eur_mwh END) AS diferencia
FROM precios_electricidad
WHERE mes = '2026-11'
GROUP BY fecha_hora_utc, fecha, hora
ORDER BY fecha_hora_utc;


-- 4. Perfil horario del mes: precio medio por hora del dia
SELECT
    hora,
    ROUND(AVG(precio_eur_mwh), 2) AS precio_medio
FROM precios_electricidad
WHERE mes = '2026-11'
  AND indicador_id = '1001'
GROUP BY hora
ORDER BY hora;
