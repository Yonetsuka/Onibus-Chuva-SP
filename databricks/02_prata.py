# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata (Requisito 06)
# MAGIC * Tipos corretos, nulos e inválidos tratados
# MAGIC * `MERGE` pela chave (rodar de novo não duplica)
# MAGIC * **3 regras de qualidade por tabela**, com o total barrado por cada uma (tabela `prata_qualidade`)
# MAGIC
# MAGIC | Tabela | Chave do MERGE | Regras de qualidade |
# MAGIC |---|---|---|
# MAGIC | `prata_onibus` | `prefixo` + `ts_utc` | prefixo nulo · coordenada fora de SP · horário inválido/futuro |
# MAGIC | `prata_clima` | `cidade` + `data_hora` | temperatura fora de -10..50 °C · chuva nula/negativa · hora nula |

# COMMAND ----------

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

S = "workspace.gs"
CIDADE = "São Paulo"


def registrar_qualidade(tabela: str, total_lido: int, contagens: dict) -> None:
    """Grava quantos registros cada regra barrou (evidência para o relatório)."""
    linhas = [(tabela, regra, int(qtd or 0), int(total_lido)) for regra, qtd in contagens.items()]
    (
        spark.createDataFrame(linhas, "tabela string, regra string, total_barrado long, total_lido long")
        .withColumn("executado_em", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(f"{S}.prata_qualidade")
    )


def merge_na_prata(df, alvo: str, view: str, chave_sql: str) -> None:
    if not spark.catalog.tableExists(alvo):
        df.limit(0).write.saveAsTable(alvo)  # cria a tabela com o schema certo
    df.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {alvo} AS alvo
        USING {view} AS novo
        ON {chave_sql}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# COMMAND ----------

# MAGIC %md ## Ônibus (SPTrans)
# MAGIC O JSON da Olho Vivo é aninhado: `l` (linhas) → `vs` (veículos). Fazemos `explode` duas vezes.

# COMMAND ----------

b = spark.table(f"{S}.bronze_onibus")

veiculos = (
    b.select("arquivo_origem", F.explode("l").alias("linha"))
    .select(
        "arquivo_origem",
        F.col("linha.cl").cast("int").alias("cod_linha"),
        F.coalesce(F.col("linha.c"), F.lit("N/D")).alias("letreiro"),  # nulo -> "N/D"
        F.col("linha.sl").cast("int").alias("sentido"),
        F.col("linha.lt0").alias("destino"),
        F.col("linha.lt1").alias("origem"),
        F.explode("linha.vs").alias("v"),
    )
    .select(
        "arquivo_origem",
        "cod_linha",
        "letreiro",
        "sentido",
        "destino",
        "origem",
        F.col("v.p").cast("string").alias("prefixo"),
        F.coalesce(F.col("v.a"), F.lit(False)).alias("acessivel"),  # nulo -> não acessível
        F.col("v.ta").cast("timestamp").alias("ts_utc"),
        F.col("v.py").cast("double").alias("lat"),
        F.col("v.px").cast("double").alias("lon"),
    )
    .withColumn("ts_local", F.from_utc_timestamp("ts_utc", "America/Sao_Paulo"))
)

# --- 3 regras de qualidade -------------------------------------------------
dentro_de_sp = F.col("lat").between(-24.1, -23.2) & F.col("lon").between(-47.0, -46.2)
regras_onibus = {
    "r1_prefixo_nulo": F.col("prefixo").isNull(),
    "r2_coordenada_fora_de_sp": ~F.coalesce(dentro_de_sp, F.lit(False)),
    "r3_horario_invalido_ou_futuro": F.col("ts_utc").isNull()
    | (F.col("ts_utc") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES")),
}

marcado = veiculos
for nome, cond in regras_onibus.items():
    marcado = marcado.withColumn(nome, cond)

agg = marcado.agg(
    F.count("*").alias("total_lido"),
    *[F.sum(F.col(r).cast("int")).alias(r) for r in regras_onibus],
).collect()[0]
total_lido = agg["total_lido"]
contagens = {r: agg[r] for r in regras_onibus}

invalido = reduce(lambda a, c: a | c, [F.col(r) for r in regras_onibus])
rejeitados = marcado.filter(invalido)
validos = (
    marcado.filter(~invalido)
    .drop(*regras_onibus)
    .dropDuplicates(["prefixo", "ts_utc"])  # MERGE não aceita chave repetida no lote
)

rejeitados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.prata_onibus_rejeitados")
registrar_qualidade("prata_onibus", total_lido, contagens)
print("Lidos:", total_lido, "| barrados por regra:", contagens, "| válidos:", validos.count())

# COMMAND ----------

antes = spark.table(f"{S}.prata_onibus").count() if spark.catalog.tableExists(f"{S}.prata_onibus") else 0
merge_na_prata(
    validos,
    f"{S}.prata_onibus",
    "lote_prata_onibus",
    "alvo.prefixo = novo.prefixo AND alvo.ts_utc = novo.ts_utc",
)
depois = spark.table(f"{S}.prata_onibus").count()
print(f"prata_onibus: {antes} -> {depois} linhas (rodar de novo mantém {depois})")

# COMMAND ----------

# MAGIC %md ## Clima (Open-Meteo)
# MAGIC O Open-Meteo devolve **listas paralelas** (`time[]`, `temperature_2m[]`, `precipitation[]`).
# MAGIC `arrays_zip` junta as listas e `explode` vira uma linha por hora.
# MAGIC Os arquivos se sobrepõem (janela de 48 h); ficamos com a versão do arquivo mais recente.

# COMMAND ----------

bc = spark.table(f"{S}.bronze_clima")

horas = (
    bc.select(
        "arquivo_origem",
        "carregado_em",
        F.explode(
            F.arrays_zip(
                F.col("hourly.time").alias("time"),
                F.col("hourly.temperature_2m").alias("temp"),
                F.col("hourly.precipitation").alias("chuva"),
            )
        ).alias("h"),
    )
    .select(
        "arquivo_origem",
        "carregado_em",
        F.lit(CIDADE).alias("cidade"),
        F.to_timestamp("h.time").alias("data_hora"),  # já vem no fuso de SP (timezone=...)
        F.col("h.temp").cast("double").alias("temp_c"),
        F.col("h.chuva").cast("double").alias("precip_mm"),
    )
)

regras_clima = {
    "r1_temperatura_fora_de_faixa": ~F.coalesce(F.col("temp_c").between(-10, 50), F.lit(False)),
    "r2_chuva_nula_ou_negativa": F.col("precip_mm").isNull() | (F.col("precip_mm") < 0),
    "r3_hora_nula": F.col("data_hora").isNull(),
}
marcado_c = horas
for nome, cond in regras_clima.items():
    marcado_c = marcado_c.withColumn(nome, cond)

agg_c = marcado_c.agg(
    F.count("*").alias("total_lido"),
    *[F.sum(F.col(r).cast("int")).alias(r) for r in regras_clima],
).collect()[0]
contagens_c = {r: agg_c[r] for r in regras_clima}
invalido_c = reduce(lambda a, c: a | c, [F.col(r) for r in regras_clima])

ultima_versao = Window.partitionBy("cidade", "data_hora").orderBy(
    F.col("carregado_em").desc(), F.col("arquivo_origem").desc()
)
validos_c = (
    marcado_c.filter(~invalido_c)
    .drop(*regras_clima)
    .withColumn("_rn", F.row_number().over(ultima_versao))
    .filter("_rn = 1")
    .drop("_rn")
)

registrar_qualidade("prata_clima", agg_c["total_lido"], contagens_c)
print("Lidos:", agg_c["total_lido"], "| barrados por regra:", contagens_c, "| válidos:", validos_c.count())

# COMMAND ----------

antes_c = spark.table(f"{S}.prata_clima").count() if spark.catalog.tableExists(f"{S}.prata_clima") else 0
merge_na_prata(
    validos_c,
    f"{S}.prata_clima",
    "lote_prata_clima",
    "alvo.cidade = novo.cidade AND alvo.data_hora = novo.data_hora",
)
depois_c = spark.table(f"{S}.prata_clima").count()
print(f"prata_clima: {antes_c} -> {depois_c} linhas")

# COMMAND ----------

# MAGIC %md ## Evidência de qualidade (print para o relatório)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tabela, regra, total_barrado, total_lido, executado_em
# MAGIC FROM workspace.gs.prata_qualidade
# MAGIC ORDER BY executado_em DESC, tabela, regra;
