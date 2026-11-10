resource "aws_glue_catalog_database" "datalake" {
  name = replace(var.proyecto, "-", "_")
}

resource "aws_glue_job" "transformar_precios" {
  name              = "${var.proyecto}-transformar-precios"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 15 # minutos: si se cuelga, no sigue facturando
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.datalake.id}/${aws_s3_object.script_glue.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--BUCKET"       = aws_s3_bucket.datalake.id
    "--MES"          = "auto"
    "--TempDir"      = "s3://${aws_s3_bucket.datalake.id}/tmp/"
  }
}

# Tabla con "partition projection": Athena calcula las particiones (mes=YYYY-MM) a partir
# de estas propiedades, sin crawlers ni MSCK REPAIR TABLE. Menos coste y nada que mantener.
resource "aws_glue_catalog_table" "precios" {
  name          = "precios_electricidad"
  database_name = aws_glue_catalog_database.datalake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"               = "parquet"
    "projection.enabled"           = "true"
    "projection.mes.type"          = "date"
    "projection.mes.format"        = "yyyy-MM"
    "projection.mes.range"         = "2024-01,NOW"
    "projection.mes.interval"      = "1"
    "projection.mes.interval.unit" = "MONTHS"
    "storage.location.template"    = "s3://${aws_s3_bucket.datalake.id}/processed/ree/precios/mes=$${mes}/"
  }

  partition_keys {
    name = "mes"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datalake.id}/processed/ree/precios/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "indicador_id"
      type = "string"
    }
    columns {
      name = "indicador"
      type = "string"
    }
    columns {
      name    = "fecha_hora_utc"
      type    = "timestamp"
      comment = "Inicio de la franja en UTC"
    }
    columns {
      name    = "fecha"
      type    = "date"
      comment = "Fecha local (Europe/Madrid)"
    }
    columns {
      name    = "hora"
      type    = "int"
      comment = "Hora local (Europe/Madrid)"
    }
    columns {
      name = "precio_eur_mwh"
      type = "double"
    }
  }
}
