# Un unico bucket con prefijos por capa: raw/ (JSON tal cual), processed/ (Parquet),
# scripts/ (codigo de Glue), tmp/ (temporales de Glue) y athena-results/.
resource "random_id" "sufijo" {
  byte_length = 4
}

resource "aws_s3_bucket" "datalake" {
  bucket        = "${var.proyecto}-${random_id.sufijo.hex}"
  force_destroy = true # proyecto de portfolio: permite "terraform destroy" con datos dentro
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "borrar-resultados-athena"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "borrar-temporales-glue"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = 1
    }
  }
}

resource "aws_s3_object" "script_glue" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/transformar_precios.py"
  source = "${path.module}/../src/glue_jobs/transformar_precios.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/transformar_precios.py")
}
