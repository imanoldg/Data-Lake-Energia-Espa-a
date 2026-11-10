# Cómo ejecutar el proyecto

Requisitos: Python 3.11, Java 17 (para PySpark) y Git. Terraform y una cuenta de AWS solo
hacen falta para el despliegue (paso 4), que es opcional.

## 1. Entorno y tests

```bash
python -m venv .venv
```
Activar el entorno, en Windows (PowerShell):
```bash
.venv\Scripts\Activate.ps1
```
o en Linux, macOS o WSL:
```bash
source .venv/bin/activate
```
```bash
pip install -r requirements-dev.txt
```
```bash
pytest -v
```

## 2. Pipeline completo en local (sin AWS)

Descargar datos reales de REE a `data/raw/`:
```bash
python src/ingesta/ingesta_ree.py --dir-local data --desde 2026-11-01 --hasta 2026-11-09
```
Transformarlos a Parquet en `data/processed/`:
```bash
python scripts/transformar_local.py --mes 2026-11
```

> En Windows, Spark necesita `winutils.exe` para escribir ficheros. Lo más sencillo es
> ejecutar este paso en WSL2.

## 3. Ingesta contra un S3 simulado (LocalStack o moto server)

```bash
aws --endpoint-url http://localhost:4566 s3 mb s3://datalake-local
```
```bash
python src/ingesta/ingesta_ree.py --bucket datalake-local --endpoint-url http://localhost:4566 --desde 2026-11-01 --hasta 2026-11-09
```

## 4. Despliegue en AWS (opcional, tiene coste)

Una ejecución del job de Glue cuesta del orden de 0,03 USD. Configura una alerta en AWS Budgets
antes de empezar.

```bash
cd infra
```
```bash
terraform init
```
```bash
terraform apply
```
```bash
python ../src/ingesta/ingesta_ree.py --bucket <bucket de terraform output> --desde 2026-11-01 --hasta 2026-11-09
```
```bash
aws glue start-job-run --job-name datalake-energia-transformar-precios
```

Después, en Athena, elige el workgroup `datalake-energia` y la base de datos `datalake_energia`
y ejecuta las consultas de [`sql/consultas.sql`](../sql/consultas.sql).

Para borrar todos los recursos y dejar de pagar:
```bash
terraform destroy
```
