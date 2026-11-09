"""
Testa as duas fontes de dados ANTES de subir o Airflow.

Uso (precisa só de Python 3, sem instalar nada):
    python scripts/testar_fontes.py

O token é pedido na hora e NÃO é salvo em arquivo.
"""
import getpass
import json
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar

SPTRANS = "http://api.olhovivo.sptrans.com.br/v2.1"
METEO = (
    "https://api.open-meteo.com/v1/forecast?latitude=-23.55&longitude=-46.63"
    "&hourly=temperature_2m,precipitation&timezone=America/Sao_Paulo&forecast_days=1"
)


def testar_open_meteo() -> None:
    print("\n[1/2] Open-Meteo (não precisa de chave)...")
    try:
        with urllib.request.urlopen(METEO, timeout=30) as r:
            dados = json.load(r)
        horas = dados["hourly"]["time"]
        print(f"  OK: {len(horas)} horas recebidas. Primeira: {horas[0]}")
    except Exception as exc:
        print("  FALHOU:", exc)


def testar_sptrans(token: str) -> None:
    print("\n[2/2] SPTrans Olho Vivo...")
    cookies = CookieJar()
    abrir = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies)).open
    try:
        url = f"{SPTRANS}/Login/Autenticar?token={urllib.parse.quote(token)}"
        req = urllib.request.Request(url, data=b"", method="POST")
        with abrir(req, timeout=30) as r:
            resposta = r.read().decode().strip().lower()
        if resposta != "true":
            print("  FALHOU no login: a API respondeu", repr(resposta))
            print("  -> token errado ou ainda não ativado.")
            return
        print("  Login OK.")
        with abrir(f"{SPTRANS}/Posicao", timeout=60) as r:
            dados = json.load(r)
        linhas = dados.get("l", [])
        veiculos = sum(len(l.get("vs", [])) for l in linhas)
        print(f"  OK: {len(linhas)} linhas e {veiculos} ônibus na rua agora.")
        if not linhas:
            print("  Aviso: lista vazia. Pode ser madrugada; tente em horário comercial.")
    except Exception as exc:
        print("  FALHOU:", exc)


if __name__ == "__main__":
    testar_open_meteo()
    token = getpass.getpass("\nCole o token da SPTrans (não aparece na tela) e dê Enter: ").strip()
    testar_sptrans(token)
    print("\nSe os dois deram OK, pode seguir para o Airflow.")
