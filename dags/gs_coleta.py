"""
DAG gs_coleta - Requisitos 01, 02 e 03 (Big Data Architecture & Data Integration)

Coleta duas fontes reais do problema "ônibus x chuva em São Paulo":
  1. SPTrans Olho Vivo  -> posição de todos os ônibus agora (API com token gratuito)
  2. Open-Meteo         -> temperatura e chuva por hora (API sem chave)

Cada coleta vira UM arquivo na pasta chegada/, com a data (e hora) no nome:
  chegada/onibus/onibus_2026-11-09_07h.json
  chegada/clima/clima_2026-11-09_07h.json

Os arquivos são enviados depois ao Volume do Databricks, em lotes (upload manual
pela interface, porque o Databricks Free Edition não conecta ao Airflow).

Airflow 3.x. Credenciais ficam em Variables (Admin > Variables), NUNCA no código:
  SPTRANS_TOKEN   (obrigatória)  token da API Olho Vivo
  GS_CHEGADA_DIR  (opcional)     pasta de pouso; padrão /opt/airflow/chegada
  ALERTA_WEBHOOK  (opcional)     URL de webhook (Teams/Discord/Slack) p/ notificar falhas

IMPORTANTE: a Olho Vivo só mostra o "agora" (não tem histórico). Por isso o DAG
usa catchup=False e o nome do arquivo usa a hora REAL da coleta. Para ter 2 lotes
de dias diferentes é preciso deixar o DAG rodando em pelo menos 2 dias.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

log = logging.getLogger(__name__)

TZ = "America/Sao_Paulo"
SPTRANS_BASE = "http://api.olhovivo.sptrans.com.br/v2.1"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
# Centro de São Paulo. Troque se o problema da equipe for outra região.
LATITUDE, LONGITUDE = -23.55, -46.63


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _chegada() -> Path:
    return Path(Variable.get("GS_CHEGADA_DIR", default="/opt/airflow/chegada"))


def _carimbo() -> str:
    """Data e hora reais da coleta, no fuso de SP. Ex.: 2026-11-09_07h"""
    return pendulum.now(TZ).format("YYYY-MM-DD_HH[h]")


def _gravar(destino: Path, texto: str) -> str:
    """Grava de forma atômica (arquivo temporário + rename) e devolve o caminho."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".tmp")
    tmp.write_text(texto, encoding="utf-8")
    tmp.replace(destino)
    return str(destino)


def notificar_falha(context) -> None:
    """Notificação: sempre loga; se ALERTA_WEBHOOK existir, avisa o time."""
    ti = context.get("ti") or context.get("task_instance")
    msg = f"[gs_coleta] falha na tarefa {getattr(ti, 'task_id', '?')}"
    log.error(msg)
    webhook = Variable.get("ALERTA_WEBHOOK", default=None)
    if webhook:
        try:
            requests.post(webhook, json={"text": msg}, timeout=10)
        except requests.RequestException as exc:  # a notificação não pode derrubar o DAG
            log.warning("Não foi possível enviar a notificação: %s", exc)


# --------------------------------------------------------------------------- #
# Tarefas de coleta (rodam em paralelo)
# --------------------------------------------------------------------------- #
def coletar_sptrans(**_) -> str:
    token = Variable.get("SPTRANS_TOKEN")  # nunca no código
    with requests.Session() as sessao:  # a API usa cookie de sessão
        login = sessao.post(
            f"{SPTRANS_BASE}/Login/Autenticar", params={"token": token}, timeout=30
        )
        login.raise_for_status()
        if login.text.strip().lower() != "true":
            raise RuntimeError("SPTrans recusou o token (Login/Autenticar != true)")
        resp = sessao.get(f"{SPTRANS_BASE}/Posicao", timeout=60)
        resp.raise_for_status()

    dados = resp.json()  # valida que é JSON antes de gravar
    log.info("SPTrans: %d linhas com ônibus na rua", len(dados.get("l", [])))
    destino = _chegada() / "onibus" / f"onibus_{_carimbo()}.json"
    return _gravar(destino, json.dumps(dados, ensure_ascii=False, separators=(",", ":")))


def coletar_clima(**_) -> str:
    resp = requests.get(
        OPEN_METEO_URL,
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": "temperature_2m,precipitation",
            "timezone": TZ,
            "past_days": 1,  # 48 h de janela: arquivos se sobrepõem de propósito,
            "forecast_days": 1,  # quem remove a repetição é o MERGE da Prata
        },
        timeout=30,
    )
    resp.raise_for_status()
    dados = resp.json()
    destino = _chegada() / "clima" / f"clima_{_carimbo()}.json"
    return _gravar(destino, json.dumps(dados, ensure_ascii=False, separators=(",", ":")))


# --------------------------------------------------------------------------- #
# Tarefa que confere se os arquivos chegaram
# --------------------------------------------------------------------------- #
def conferir_chegada(ti, **_) -> None:
    caminhos = ti.xcom_pull(task_ids=["coletar_sptrans", "coletar_clima"])
    problemas = []
    for caminho in caminhos:
        arq = Path(caminho) if caminho else None
        if arq is None or not arq.exists() or arq.stat().st_size == 0:
            problemas.append(f"ausente ou vazio: {caminho}")
            continue
        conteudo = json.loads(arq.read_text(encoding="utf-8"))
        if arq.parent.name == "onibus" and not conteudo.get("l"):
            problemas.append(f"SPTrans sem nenhuma linha em operação: {arq.name}")
        if arq.parent.name == "clima" and not conteudo.get("hourly", {}).get("time"):
            problemas.append(f"Open-Meteo sem dados horários: {arq.name}")
    if problemas:
        raise RuntimeError("Conferência falhou: " + "; ".join(problemas))
    log.info("Conferência OK: %s", ", ".join(Path(c).name for c in caminhos))


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
with DAG(
    dag_id="gs_coleta",
    description="Coleta SPTrans + Open-Meteo para a pasta chegada/",
    schedule="0 7,12,18 * * *",  # 3 coletas por dia (07h, 12h, 18h de SP)
    start_date=pendulum.datetime(2026, 11, 8, tz=TZ),
    catchup=False,  # Olho Vivo não tem histórico: não existe "coleta retroativa"
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        "on_failure_callback": notificar_falha,
    },
    tags=["gs", "citizentech", "big-data"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    sptrans = PythonOperator(task_id="coletar_sptrans", python_callable=coletar_sptrans)
    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    # paralelismo: as duas coletas rodam ao mesmo tempo; a conferência espera as duas
    inicio >> [sptrans, clima] >> conferir
