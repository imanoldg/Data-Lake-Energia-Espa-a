output "bucket" {
  value = aws_s3_bucket.datalake.id
}

output "glue_job" {
  value = aws_glue_job.transformar_precios.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.datalake.name
}

output "base_de_datos" {
  value = aws_glue_catalog_database.datalake.name
}
