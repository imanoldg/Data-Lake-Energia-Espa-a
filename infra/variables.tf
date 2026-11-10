variable "region" {
  description = "Region de AWS"
  type        = string
  default     = "eu-west-1"
}

variable "proyecto" {
  description = "Prefijo para nombrar todos los recursos"
  type        = string
  default     = "datalake-energia"
}

variable "glue_workers" {
  description = "Numero de workers G.1X del job de Glue (minimo 2)"
  type        = number
  default     = 2
}

variable "athena_limite_bytes" {
  description = "Maximo de bytes que puede escanear una consulta de Athena (control de costes)"
  type        = number
  default     = 1073741824 # 1 GiB
}
