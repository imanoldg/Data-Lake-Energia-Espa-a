"""Ejecuta la misma transformacion que el job de Glue, pero en local y sin AWS.

Funciona tambien en Windows SIN winutils.exe / HADOOP_HOME:
  * los JSON se leen con Python y se pasan a Spark en memoria (sin el sistema de ficheros
    de Hadoop);
  * la transformacion y la validacion son las MISMAS funciones que usa el job de Glue;
  * el Parquet se escribe con pyarrow, con el mismo particionado (mes=YYYY-MM) y la misma
    semantica de sobrescritura por particion.

Uso (despues de la ingesta con --dir-local data):
    python scripts/transformar_local.py --mes 2026-11
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

# Spark lanza procesos de Python; en Windows no existe "python3", asi que le indicamos
# explicitamente el interprete del entorno virtual actual.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pyarrow as pa  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from glue_jobs.transformar_precios import (  # noqa: E402
    RUTA_PROCESSED,
    RUTA_RAW,
    transformar_precios,
    validar,
)


def leer_json_raw(dir_base: Path, mes: str) -> list[str]:
    ficheros = sorted((dir_base / RUTA_RAW).glob(f"fecha={mes}-*/*.json"))
    return [f.read_text(encoding="utf-8") for f in ficheros]


def escribir_parquet(df_pandas, salida: Path) -> None:
    """Escribe particionado por mes, reemplazando solo las particiones presentes."""
    tabla = pa.Table.from_pandas(df_pandas, preserve_index=False)
    ds.write_dataset(
        tabla,
        salida,
        format="parquet",
        partitioning=["mes"],
        partitioning_flavor="hive",  # carpetas mes=2026-11, como en S3
        existing_data_behavior="delete_matching",  # = overwrite dinamico de Spark
        basename_template="part-{i}.parquet",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True, help="Mes a procesar (YYYY-MM)")
    parser.add_argument("--dir", type=Path, default=RAIZ / "data", help="Carpeta del lake local")
    args = parser.parse_args()

    documentos = leer_json_raw(args.dir, args.mes)
    if not documentos:
        print(f"No hay ficheros en {args.dir / RUTA_RAW} para el mes {args.mes}.")
        print("Ejecuta antes la ingesta con --dir-local.")
        return 1
    print(f"{len(documentos)} ficheros diarios encontrados para {args.mes}")

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("transformar-precios-local")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df_raw = spark.read.json(spark.sparkContext.parallelize(documentos))
        df = transformar_precios(df_raw).where(F.col("mes") == args.mes).cache()

        errores = validar(df)
        if errores:
            print("Controles de calidad fallidos:", *errores, sep="\n  - ")
            return 1

        df.orderBy("indicador_id", "fecha_hora_utc").show(10, truncate=False)
        salida = args.dir / RUTA_PROCESSED
        escribir_parquet(df.toPandas(), salida)
        print(f"{df.count()} filas escritas en {salida / f'mes={args.mes}'}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
