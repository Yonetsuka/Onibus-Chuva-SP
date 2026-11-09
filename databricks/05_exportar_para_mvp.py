# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Exportar a Ouro para o MVP (Requisito 08: contrato com o produto)
# MAGIC Gera `ouro.json` no Volume. Baixe pela interface (**Catalog > workspace > gs > chegada > _export**)
# MAGIC e coloque em `mvp/data/ouro.json` no repositório. O MVP lê somente esse arquivo.
# MAGIC
# MAGIC **Contrato do JSON** (o que o MVP consome e mostra):
# MAGIC ```
# MAGIC { gerado_em, fonte, simulado: false,
# MAGIC   resumo_chuva: [{faixa, observacoes, media_onibus, variacao_pct}],
# MAGIC   linhas: [{id, letreiro, origem, destino, ativos_agora, acessiveis_agora,
# MAGIC             horas: [{hora, qtd_onibus, pct_acessivel, precip_mm}]}] }
# MAGIC ```

# COMMAND ----------

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

S = "workspace.gs"
TOP_LINHAS = 60  # o MVP fica leve e rápido em celular de entrada

fato = spark.table(f"{S}.fato_oferta_linha_hora")
dim_l = spark.table(f"{S}.dim_linha")
dim_t = spark.table(f"{S}.dim_tempo")
rec = spark.table(f"{S}.posicao_recente")

# linhas com maior oferta média
top = (
    fato.groupBy("linha_id").agg(F.avg("qtd_onibus").alias("m")).orderBy(F.desc("m")).limit(TOP_LINHAS)
)

horas = (
    fato.join(top.select("linha_id"), "linha_id")
    .join(dim_t, "tempo_id")
    .select(
        "linha_id",
        F.date_format("hora", "yyyy-MM-dd'T'HH:mm").alias("hora"),
        "qtd_onibus",
        "pct_acessivel",
        F.coalesce("precip_mm", F.lit(0.0)).alias("precip_mm"),
    )
    .orderBy("linha_id", "hora")
    .toPandas()
)

agora = (
    rec.groupBy("cod_linha")
    .agg(F.count("*").alias("ativos_agora"), F.sum(F.col("acessivel").cast("int")).alias("acessiveis_agora"))
    .withColumnRenamed("cod_linha", "linha_id")
    .toPandas()
    .set_index("linha_id")
)

linhas_pd = dim_l.join(top.select("linha_id"), "linha_id").toPandas()

linhas = []
for _, r in linhas_pd.iterrows():
    lid = int(r["linha_id"])
    h = horas[horas["linha_id"] == lid]
    ag = agora.loc[lid] if lid in agora.index else None
    linhas.append(
        {
            "id": lid,
            "letreiro": r["letreiro"],
            "origem": r["origem"],
            "destino": r["destino"],
            "ativos_agora": int(ag["ativos_agora"]) if ag is not None else 0,
            "acessiveis_agora": int(ag["acessiveis_agora"]) if ag is not None else 0,
            "horas": [
                {
                    "hora": x.hora,
                    "qtd_onibus": int(x.qtd_onibus),
                    "pct_acessivel": float(x.pct_acessivel),
                    "precip_mm": float(x.precip_mm),
                }
                for x in h.itertuples()
            ],
        }
    )
linhas.sort(key=lambda l: l["letreiro"])

resumo = (
    fato.filter("faixa_chuva <> 'sem dado'")
    .groupBy("faixa_chuva")
    .agg(F.count("*").alias("observacoes"), F.round(F.avg("qtd_onibus"), 2).alias("media_onibus"))
    .toPandas()
)
ref = resumo.loc[resumo["faixa_chuva"] == "sem chuva", "media_onibus"]
ref = float(ref.iloc[0]) if len(ref) else None
ordem = {"sem chuva": 1, "fraca": 2, "moderada": 3, "forte": 4}
resumo_chuva = [
    {
        "faixa": r.faixa_chuva,
        "observacoes": int(r.observacoes),
        "media_onibus": float(r.media_onibus),
        "variacao_pct": round((float(r.media_onibus) / ref - 1) * 100, 1) if ref else None,
    }
    for r in sorted(resumo.itertuples(), key=lambda x: ordem.get(x.faixa_chuva, 9))
]

saida = {
    "gerado_em": datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="minutes"),
    "fonte": "SPTrans Olho Vivo + Open-Meteo, via pipeline Bronze/Prata/Ouro",
    "simulado": False,
    "resumo_chuva": resumo_chuva,
    "linhas": linhas,
}

# COMMAND ----------

import os

pasta = "/Volumes/workspace/gs/chegada/_export"
os.makedirs(pasta, exist_ok=True)
with open(f"{pasta}/ouro.json", "w", encoding="utf-8") as f:
    json.dump(saida, f, ensure_ascii=False)
print(f"ouro.json gerado: {len(linhas)} linhas, {sum(len(l['horas']) for l in linhas)} medições")
