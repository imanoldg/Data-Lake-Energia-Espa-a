import json
import os
import sys
from datetime import date

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from glue_jobs.transformar_precios import (  # noqa: E402
    COLUMNAS_SALIDA,
    transformar_precios,
    validar,
)


@pytest.fixture(scope="module")
def spark():
    # En Windows no existe "python3": Spark debe usar el interprete del entorno actual
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    sesion = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()


@pytest.fixture
def df_raw(spark, payload_ree):
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(payload_ree)]))


def test_aplana_una_fila_por_indicador_y_hora(df_raw):
    df = transformar_precios(df_raw)
    assert df.columns == COLUMNAS_SALIDA
    assert df.count() == 6
    assert {r.indicador_id for r in df.collect()} == {"1001", "600"}


def test_convierte_hora_local_a_utc(df_raw):
    # Formateamos el timestamp DENTRO de Spark (zona de sesion = UTC). Si lo comparasemos
    # como datetime de Python, PySpark lo convertiria a la zona horaria del sistema operativo
    # y el test dependeria de la maquina donde se ejecuta.
    fila = (
        transformar_precios(df_raw)
        .where("indicador_id = '1001' AND hora = 0")
        .withColumn("utc_texto", F.date_format("fecha_hora_utc", "yyyy-MM-dd HH:mm"))
        .first()
    )
    # 00:00 en Madrid (+01:00 en noviembre) son las 23:00 UTC del dia anterior
    assert fila.utc_texto == "2026-11-07 23:00"
    assert fila.fecha == date(2026, 11, 8)
    assert fila.mes == "2026-11"
    assert fila.precio_eur_mwh == pytest.approx(112.5)


def test_es_idempotente_ante_datos_duplicados(df_raw):
    df = transformar_precios(df_raw.unionByName(df_raw))
    assert df.count() == 6


def test_validar_acepta_datos_correctos(df_raw):
    assert validar(transformar_precios(df_raw)) == []


def test_validar_detecta_precios_fuera_de_rango(spark, payload_ree):
    payload_ree["included"][0]["attributes"]["values"][0]["value"] = 99999
    df_raw = spark.read.json(spark.sparkContext.parallelize([json.dumps(payload_ree)]))
    errores = validar(transformar_precios(df_raw))
    assert len(errores) == 1
    assert "fuera del rango" in errores[0]


def test_validar_detecta_resultado_vacio(df_raw):
    assert "El resultado esta vacio" in validar(transformar_precios(df_raw).limit(0))
