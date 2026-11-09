"""
Gera DADOS SIMULADOS no mesmo formato das APIs reais, só para você:
  * abrir o MVP agora (mvp/data/ouro.json) e
  * testar os notebooks antes da primeira coleta real (amostras/).

Quando o pipeline real rodar, SUBSTITUA:
  - amostras/*.json  por um pouco de cada arquivo real da pasta chegada/
  - mvp/data/ouro.json  pelo ouro.json exportado no notebook 05

Uso:  python scripts/gerar_amostras_simuladas.py
"""
import json
import math
import random
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
rng = random.Random(42)

# (cl, letreiro, origem, destino, frota_base) - nomes ilustrativos, NÃO são linhas reais
LINHAS = [
    (1001, "1012-10", "Terminal A", "Terminal B", 14),
    (1002, "1012-21", "Terminal B", "Terminal A", 13),
    (1003, "2045-10", "Praça Central", "Metrô Norte", 10),
    (1004, "2045-21", "Metrô Norte", "Praça Central", 9),
    (1005, "3120-10", "Hospital Sul", "Terminal A", 7),
    (1006, "3120-21", "Terminal A", "Hospital Sul", 7),
    (1007, "4007-10", "Parque Leste", "Estação Oeste", 11),
    (1008, "4007-21", "Estação Oeste", "Parque Leste", 11),
    (1009, "5501-10", "Vila Nova", "Centro", 6),
    (1010, "5501-21", "Centro", "Vila Nova", 6),
    (1011, "6230-10", "Terminal B", "Universidade", 8),
    (1012, "6230-21", "Universidade", "Terminal B", 8),
]
DIAS = ["2026-11-09", "2026-11-10", "2026-11-11"]
# chuva simulada (mm/h): dia -> {hora: mm}
CHUVA = {
    "2026-11-09": {19: 0.4},
    "2026-11-10": {14: 1.8, 15: 5.2, 16: 7.9, 17: 3.1},
    "2026-11-11": {8: 0.6, 9: 1.1},
}


def perfil(h: int) -> float:
    """Curva diária: picos às 7h e 18h."""
    return 0.45 + 0.55 * max(
        math.exp(-((h - 7.5) ** 2) / 8), math.exp(-((h - 18) ** 2) / 10)
    ) + 0.1 * (6 <= h <= 22)


def oferta(base: int, h: int, mm: float) -> int:
    queda = 0.0 if mm <= 0 else min(0.04 * mm + 0.05, 0.30)  # chuva reduz a oferta
    v = base * perfil(h) * (1 - queda) + rng.uniform(-0.7, 0.7)
    return max(1, round(v))


# ---------------------------------------------------------------- ouro.json
linhas_json = []
tot = {}
for cl, let, ori, des, base in LINHAS:
    horas = []
    for d in DIAS:
        for h in range(6, 23):
            mm = CHUVA[d].get(h, 0.0)
            n = oferta(base, h, mm)
            pct = round(min(100, 88 + rng.uniform(-6, 8) - (4 if h >= 21 else 0)), 1)
            horas.append({"hora": f"{d}T{h:02d}:00", "qtd_onibus": n, "pct_acessivel": pct, "precip_mm": mm})
            faixa = "sem chuva" if mm <= 0 else "fraca" if mm <= 2.5 else "moderada" if mm <= 7.6 else "forte"
            tot.setdefault(faixa, []).append(n)
    ativos = max(1, round(base * 0.8))
    linhas_json.append(
        {
            "id": cl,
            "letreiro": let,
            "origem": ori,
            "destino": des,
            "ativos_agora": ativos,
            "acessiveis_agora": round(ativos * 0.9),
            "horas": horas,
        }
    )

ref = sum(tot["sem chuva"]) / len(tot["sem chuva"])
resumo = []
for faixa in ["sem chuva", "fraca", "moderada", "forte"]:
    if faixa in tot:
        m = sum(tot[faixa]) / len(tot[faixa])
        resumo.append(
            {
                "faixa": faixa,
                "observacoes": len(tot[faixa]),
                "media_onibus": round(m, 2),
                "variacao_pct": round((m / ref - 1) * 100, 1),
            }
        )

ouro = {
    "gerado_em": "2026-11-11T23:00-03:00",
    "fonte": "DADOS SIMULADOS no formato da Ouro (troque pelo ouro.json real do notebook 05)",
    "simulado": True,
    "resumo_chuva": resumo,
    "linhas": linhas_json,
}
(RAIZ / "mvp" / "data").mkdir(parents=True, exist_ok=True)
(RAIZ / "mvp" / "data" / "ouro.json").write_text(json.dumps(ouro, ensure_ascii=False), encoding="utf-8")

# ------------------------------------------------- amostras no formato das APIs
(RAIZ / "amostras").mkdir(exist_ok=True)
veic = []
for cl, let, ori, des, base in LINHAS[:3]:
    vs = [
        {
            "p": str(60000 + cl * 10 + i),
            "a": rng.random() < 0.9,
            "ta": "2026-11-09T10:00:05Z",
            "py": round(-23.55 + rng.uniform(-0.05, 0.05), 6),
            "px": round(-46.63 + rng.uniform(-0.05, 0.05), 6),
        }
        for i in range(4)
    ]
    veic.append({"c": let, "cl": cl, "sl": 1, "lt0": des, "lt1": ori, "qv": len(vs), "vs": vs})
(RAIZ / "amostras" / "onibus_EXEMPLO_2026-11-09_07h.json").write_text(
    json.dumps({"hr": "07:00", "l": veic}, ensure_ascii=False, indent=1), encoding="utf-8"
)

horas_ref = [f"2026-11-09T{h:02d}:00" for h in range(24)]
clima = {
    "latitude": -23.55,
    "longitude": -46.63,
    "utc_offset_seconds": -10800,
    "timezone": "America/Sao_Paulo",
    "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "precipitation": "mm"},
    "hourly": {
        "time": horas_ref,
        "temperature_2m": [round(17 + 9 * math.sin((h - 9) / 24 * 2 * math.pi) ** 2, 1) for h in range(24)],
        "precipitation": [CHUVA["2026-11-09"].get(h, 0.0) for h in range(24)],
    },
}
(RAIZ / "amostras" / "clima_EXEMPLO_2026-11-09_07h.json").write_text(
    json.dumps(clima, ensure_ascii=False, indent=1), encoding="utf-8"
)
print("OK: mvp/data/ouro.json e amostras/ gerados (dados SIMULADOS)")
