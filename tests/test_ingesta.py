import copy
from datetime import date

import boto3
import pytest
from moto import mock_aws

from ingesta import ingesta_ree as ing


def test_construir_params_cubre_el_dia_completo():
    assert ing.construir_params(date(2026, 11, 8)) == {
        "start_date": "2026-11-08T00:00",
        "end_date": "2026-11-08T23:59",
        "time_trunc": "hour",
    }


def test_clave_raw_es_determinista_y_particionada():
    assert ing.clave_raw(date(2026, 11, 8)) == "raw/ree/precios/fecha=2026-11-08/precios.json"


def test_validar_respuesta_cuenta_valores(payload_ree):
    assert ing.validar_respuesta(payload_ree) == 6


@pytest.mark.parametrize(
    "romper",
    [
        lambda p: p.pop("included"),
        lambda p: p.update(included=[]),
        lambda p: p["included"][0]["attributes"].pop("values"),
        lambda p: [i["attributes"].update(values=[]) for i in p["included"]],
    ],
    ids=["sin_included", "included_vacio", "indicador_sin_values", "sin_valores"],
)
def test_validar_respuesta_rechaza_payloads_invalidos(payload_ree, romper):
    payload = copy.deepcopy(payload_ree)
    romper(payload)
    with pytest.raises(ValueError):
        ing.validar_respuesta(payload)


def test_rango_fechas():
    fechas = ing.rango_fechas(date(2026, 10, 30), date(2026, 11, 2))
    assert fechas == [date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1), date(2026, 11, 2)]
    with pytest.raises(ValueError):
        ing.rango_fechas(date(2026, 11, 2), date(2026, 11, 1))


@mock_aws
def test_guardar_s3_escribe_en_raw(payload_ree):
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(
        Bucket="lake-test", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    clave = ing.clave_raw(date(2026, 11, 8))

    ing.guardar_s3(payload_ree, "lake-test", clave, s3)
    ing.guardar_s3(payload_ree, "lake-test", clave, s3)  # re-ejecucion: sobrescribe

    objetos = s3.list_objects_v2(Bucket="lake-test", Prefix="raw/")["Contents"]
    assert [o["Key"] for o in objetos] == [clave]


class SesionFalsa:
    def __init__(self, payload):
        self.payload = payload
        self.llamadas = []

    def get(self, url, params, timeout):
        self.llamadas.append(params)
        payload = self.payload

        class Respuesta:
            def raise_for_status(self):
                pass

            def json(self):
                return payload

        return Respuesta()


def test_main_backfill_en_local(tmp_path, payload_ree, monkeypatch):
    sesion = SesionFalsa(payload_ree)
    monkeypatch.setattr(ing, "crear_sesion", lambda: sesion)

    codigo = ing.main(
        ["--dir-local", str(tmp_path), "--desde", "2026-11-07", "--hasta", "2026-11-08"]
    )

    assert codigo == 0
    assert len(sesion.llamadas) == 2
    assert (tmp_path / "raw/ree/precios/fecha=2026-11-07/precios.json").exists()
    assert (tmp_path / "raw/ree/precios/fecha=2026-11-08/precios.json").exists()


def test_main_devuelve_error_si_la_respuesta_es_invalida(tmp_path, monkeypatch):
    monkeypatch.setattr(ing, "crear_sesion", lambda: SesionFalsa({"included": []}))
    assert ing.main(["--dir-local", str(tmp_path), "--desde", "2026-11-08"]) == 1
    assert not (tmp_path / "raw").exists()


def test_main_sin_credenciales_termina_con_error_controlado(payload_ree, monkeypatch):
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/no/existe")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/no/existe")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    sesion = SesionFalsa(payload_ree)
    monkeypatch.setattr(ing, "crear_sesion", lambda: sesion)

    codigo = ing.main(
        [
            "--bucket",
            "x",
            "--endpoint-url",
            "http://127.0.0.1:1",
            "--desde",
            "2026-11-01",
            "--hasta",
            "2026-11-05",
        ]
    )

    assert codigo == 1
    assert len(sesion.llamadas) == 1  # se detiene en el primer dia, no repite el error
