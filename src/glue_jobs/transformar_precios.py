"""Job de AWS Glue (PySpark): raw/ (JSON de REE) -> processed/ (Parquet particionado por mes).

Lee todos los ficheros diarios de un mes, los aplana a una fila por (indicador, instante),
valida la calidad y reescribe SOLO la particion de ese mes (overwrite dinamico). Asi:
  * re-ejecutar el job es idempotente (no duplica filas);
  * se evita el problema de "muchos ficheros pequenos" de particionar por dia.

La logica de transformacion vive en funciones puras (transformar_precios, validar) que no
dependen de awsglue, para poder testearlas en local con pyspark.

Parametros del job:
  --BUCKET  bucket del data lake
  --MES     mes a procesar (YYYY-MM) o "auto" para el mes de ayer (hora de Madrid)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RUTA_RAW = "raw/ree/precios"
RUTA_PROCESSED = "processed/ree/precios"
FORMATO_FECHA_REE = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"  # p. ej. 2026-11-08T00:00:00.000+01:00
RANGO_PRECIO_VALIDO = (-500.0, 5000.0)  # EUR/MWh; fuera de aqui es casi seguro un error

COLUMNAS_SALIDA = [
    "indicador_id",
    "indicador",
    "fecha_hora_utc",
    "fecha",
    "hora",
    "precio_eur_mwh",
    "mes",
]


def transformar_precios(df_raw: DataFrame) -> DataFrame:
    """Aplana la respuesta de REE: una fila por indicador y franja horaria.

    Requiere spark.sql.session.timeZone = UTC para que fecha_hora_utc sea realmente UTC.
    fecha, hora y mes se calculan en hora LOCAL de Madrid (la del propio dato), que es
    como se consultan los precios de la luz.
    """
    indicadores = df_raw.select(F.explode("included").alias("ind"))
    valores = indicadores.select(
        F.col("ind.id").cast("string").alias("indicador_id"),
        F.col("ind.attributes.title").alias("indicador"),
        F.explode("ind.attributes.values").alias("v"),
    )
    return (
        valores.select(
            "indicador_id",
            "indicador",
            F.col("v.datetime").alias("dt_local"),
            F.col("v.value").cast("double").alias("precio_eur_mwh"),
        )
        .where(F.col("precio_eur_mwh").isNotNull())
        .withColumn("fecha_hora_utc", F.to_timestamp("dt_local", FORMATO_FECHA_REE))
        .withColumn("fecha", F.to_date(F.substring("dt_local", 1, 10)))
        .withColumn("hora", F.substring("dt_local", 12, 2).cast("int"))
        .withColumn("mes", F.substring("dt_local", 1, 7))
        # Si un dia se ingesto dos veces o hay solapes entre ficheros, nos quedamos con uno
        .dropDuplicates(["indicador_id", "fecha_hora_utc"])
        .select(*COLUMNAS_SALIDA)
    )


def validar(df: DataFrame) -> list[str]:
    """Controles de calidad. Devuelve la lista de errores (vacia si todo esta bien)."""
    minimo, maximo = RANGO_PRECIO_VALIDO
    resumen = df.agg(
        F.count(F.lit(1)).alias("filas"),
        F.sum(F.col("fecha_hora_utc").isNull().cast("int")).alias("fechas_nulas"),
        F.sum((~F.col("precio_eur_mwh").between(minimo, maximo)).cast("int")).alias(
            "fuera_de_rango"
        ),
    ).first()

    errores = []
    if resumen["filas"] == 0:
        errores.append("El resultado esta vacio")
    if resumen["fechas_nulas"]:
        errores.append(f"{resumen['fechas_nulas']} filas con fecha no parseable")
    if resumen["fuera_de_rango"]:
        errores.append(
            f"{resumen['fuera_de_rango']} precios fuera del rango [{minimo}, {maximo}] EUR/MWh"
        )
    return errores


def mes_por_defecto() -> str:
    ayer = datetime.now(ZoneInfo("Europe/Madrid")).date() - timedelta(days=1)
    return ayer.strftime("%Y-%m")


def main() -> None:
    # Imports de Glue aqui dentro: solo existen en el runtime de AWS Glue.
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "MES"])
    mes = mes_por_defecto() if args["MES"] == "auto" else args["MES"]
    datetime.strptime(mes, "%Y-%m")  # falla pronto si el formato no es YYYY-MM

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    entrada = f"s3://{args['BUCKET']}/{RUTA_RAW}/fecha={mes}-*/"
    salida = f"s3://{args['BUCKET']}/{RUTA_PROCESSED}/"

    df_raw = spark.read.option("multiLine", "true").json(entrada)
    df = transformar_precios(df_raw).where(F.col("mes") == mes).cache()

    errores = validar(df)
    if errores:
        raise ValueError("Controles de calidad fallidos: " + "; ".join(errores))

    print(f"Escribiendo {df.count()} filas del mes {mes} en {salida}")
    df.coalesce(1).write.mode("overwrite").partitionBy("mes").parquet(salida)

    job.commit()


if __name__ == "__main__":
    main()
