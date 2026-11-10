"""Ingesta diaria de precios de la electricidad desde la API publica de Red Electrica (REE).

Descarga la respuesta JSON de un dia y la guarda SIN MODIFICAR en la capa raw del data lake:

    s3://<bucket>/raw/ree/precios/fecha=YYYY-MM-DD/precios.json

La clave depende solo de la fecha, asi que volver a ejecutar el mismo dia sobrescribe el
fichero en lugar de duplicarlo (la ingesta es idempotente).

Ejemplos:
    # Ayer (hora de Madrid), a S3 real
    python -m ingesta.ingesta_ree --bucket mi-bucket

    # Un rango de fechas (backfill) contra LocalStack o moto, con el perfil "local"
    python -m ingesta.ingesta_ree --bucket mi-bucket --desde 2026-11-01 --hasta 2026-11-07 \
        --endpoint-url http://127.0.0.1:5000 --profile local

    # Sin AWS: guardar en una carpeta local
    python -m ingesta.ingesta_ree --dir-local data
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL_REE = "https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real"
ZONA_MADRID = ZoneInfo("Europe/Madrid")
PREFIJO_RAW = "raw/ree/precios"

log = logging.getLogger("ingesta_ree")


def crear_sesion() -> requests.Session:
    """Sesion HTTP con reintentos y backoff exponencial para errores transitorios."""
    reintentos = Retry(
        total=4,
        backoff_factor=2,  # 2s, 4s, 8s, 16s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    sesion = requests.Session()
    sesion.mount("https://", HTTPAdapter(max_retries=reintentos))
    sesion.headers["Accept"] = "application/json"
    return sesion


def construir_params(fecha: date) -> dict[str, str]:
    dia = fecha.isoformat()
    return {
        "start_date": f"{dia}T00:00",
        "end_date": f"{dia}T23:59",
        "time_trunc": "hour",
    }


def descargar(fecha: date, sesion: requests.Session, timeout: int = 30) -> dict:
    respuesta = sesion.get(URL_REE, params=construir_params(fecha), timeout=timeout)
    respuesta.raise_for_status()
    return respuesta.json()


def validar_respuesta(payload: dict) -> int:
    """Comprueba la estructura minima y devuelve el numero total de valores.

    Lanza ValueError si la respuesta no tiene datos utilizables, para no
    escribir ficheros vacios o corruptos en raw/.
    """
    indicadores = payload.get("included")
    if not isinstance(indicadores, list) or not indicadores:
        raise ValueError("La respuesta no contiene la lista 'included' de indicadores")

    total = 0
    for indicador in indicadores:
        valores = indicador.get("attributes", {}).get("values")
        if not isinstance(valores, list):
            raise ValueError(f"El indicador {indicador.get('id')} no tiene 'values'")
        total += len(valores)

    if total == 0:
        raise ValueError("La respuesta no contiene ningun valor")
    return total


def clave_raw(fecha: date) -> str:
    return f"{PREFIJO_RAW}/fecha={fecha.isoformat()}/precios.json"


def guardar_s3(payload: dict, bucket: str, clave: str, s3_client) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=clave,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def guardar_local(payload: dict, dir_base: Path, clave: str) -> Path:
    destino = dir_base / clave
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return destino


def rango_fechas(desde: date, hasta: date) -> list[date]:
    if hasta < desde:
        raise ValueError("--hasta no puede ser anterior a --desde")
    return [desde + timedelta(days=i) for i in range((hasta - desde).days + 1)]


def ayer_madrid() -> date:
    return datetime.now(ZONA_MADRID).date() - timedelta(days=1)


def parsear_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingesta de precios de electricidad (REE)")
    destino = parser.add_mutually_exclusive_group(required=True)
    destino.add_argument("--bucket", help="Bucket S3 del data lake")
    destino.add_argument("--dir-local", type=Path, help="Carpeta local en lugar de S3")
    parser.add_argument("--desde", type=date.fromisoformat, help="Fecha inicial (YYYY-MM-DD)")
    parser.add_argument("--hasta", type=date.fromisoformat, help="Fecha final (YYYY-MM-DD)")
    parser.add_argument("--endpoint-url", help="Endpoint S3 alternativo, p. ej. LocalStack")
    parser.add_argument("--profile", help="Perfil de AWS CLI a usar (p. ej. 'local')")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parsear_args(argv)

    desde = args.desde or ayer_madrid()
    hasta = args.hasta or desde
    fechas = rango_fechas(desde, hasta)

    s3_client = None
    errores_s3: tuple[type[Exception], ...] = ()
    if args.bucket:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError

        sesion_aws = boto3.Session(profile_name=args.profile)
        s3_client = sesion_aws.client("s3", endpoint_url=args.endpoint_url)
        errores_s3 = (BotoCoreError, ClientError)

    sesion = crear_sesion()
    fallos = 0
    for fecha in fechas:
        clave = clave_raw(fecha)
        try:
            payload = descargar(fecha, sesion)
            n_valores = validar_respuesta(payload)
            if s3_client:
                guardar_s3(payload, args.bucket, clave, s3_client)
                log.info("%s: %d valores -> s3://%s/%s", fecha, n_valores, args.bucket, clave)
            else:
                ruta = guardar_local(payload, args.dir_local, clave)
                log.info("%s: %d valores -> %s", fecha, n_valores, ruta)
        except (requests.RequestException, ValueError) as error:
            fallos += 1
            log.error("%s: fallo en la ingesta: %s", fecha, error)
        except errores_s3 as error:
            # Credenciales, bucket inexistente, endpoint caido...: afecta a todos los dias,
            # asi que paramos en vez de repetir el mismo error N veces.
            log.error("%s: no se pudo escribir en S3: %s", fecha, error)
            return 1

    return 1 if fallos else 0


if __name__ == "__main__":
    raise SystemExit(main())
