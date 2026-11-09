# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro (Requisitos 07 e 08)
# MAGIC Modelo dimensional (estrela) que alimenta o MVP "Ônibus & Chuva".
# MAGIC
# MAGIC ```
# MAGIC dim_linha ──┐
# MAGIC             ├── fato_oferta_linha_hora  (1 linha por linha × hora)
# MAGIC dim_tempo ──┘
# MAGIC
# MAGIC posicao_recente   (tabela de apoio do MVP: última posição, com LGPD aplicada)
# MAGIC ```
# MAGIC As tabelas da Ouro são recriadas a partir da Prata a cada execução (idempotente).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

S = "workspace.gs"
po = spark.table(f"{S}.prata_onibus")
pc = spark.table(f"{S}.prata_clima")


def salvar(df, nome: str) -> None:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.{nome}")
    print(f"{nome}: {spark.table(f'{S}.{nome}').count()} linhas")


# COMMAND ----------

# MAGIC %md ## Dimensão linha

# COMMAND ----------

dim_linha = (
    po.groupBy("cod_linha")
    .agg(
        F.max("letreiro").alias("letreiro"),
        F.max("destino").alias("destino"),
        F.max("origem").alias("origem"),
        F.max("sentido").alias("sentido"),
    )
    .withColumnRenamed("cod_linha", "linha_id")
)
salvar(dim_linha, "dim_linha")

# COMMAND ----------

# MAGIC %md ## Dimensão tempo (uma linha por hora observada)

# COMMAND ----------

dim_tempo = (
    po.select(F.date_trunc("hour", "ts_local").alias("hora"))
    .distinct()
    .select(
        F.date_format("hora", "yyyyMMddHH").cast("long").alias("tempo_id"),
        "hora",
        F.to_date("hora").alias("data"),
        F.hour("hora").alias("hora_do_dia"),
        F.date_format("hora", "E").alias("dia_semana"),
        F.when(F.hour("hora") < 6, "madrugada")
        .when(F.hour("hora") < 12, "manhã")
        .when(F.hour("hora") < 18, "tarde")
        .otherwise("noite")
        .alias("periodo"),
        (
            F.dayofweek("hora").between(2, 6)  # segunda..sexta
            & (F.hour("hora").between(6, 9) | F.hour("hora").between(16, 19))
        ).alias("horario_de_pico"),
    )
)
salvar(dim_tempo, "dim_tempo")

# COMMAND ----------

# MAGIC %md ## Fato: oferta de ônibus por linha e hora, com a chuva da hora
# MAGIC Faixas de chuva (mm/h): sem chuva = 0 · fraca ≤ 2,5 · moderada ≤ 7,6 · forte > 7,6.

# COMMAND ----------

oferta = (
    po.withColumn("hora", F.date_trunc("hour", "ts_local"))
    .groupBy("cod_linha", "hora")
    .agg(
        F.countDistinct("prefixo").alias("qtd_onibus"),
        F.countDistinct(F.when(F.col("acessivel"), F.col("prefixo"))).alias("qtd_acessiveis"),
    )
)
clima = pc.select(F.col("data_hora").alias("hora"), "precip_mm", "temp_c")

fato = (
    oferta.join(clima, "hora", "left")
    .select(
        F.col("cod_linha").alias("linha_id"),
        F.date_format("hora", "yyyyMMddHH").cast("long").alias("tempo_id"),
        "qtd_onibus",
        "qtd_acessiveis",
        F.round(F.col("qtd_acessiveis") / F.col("qtd_onibus") * 100, 1).alias("pct_acessivel"),
        "precip_mm",
        "temp_c",
        F.when(F.col("precip_mm").isNull(), "sem dado")
        .when(F.col("precip_mm") <= 0, "sem chuva")
        .when(F.col("precip_mm") <= 2.5, "fraca")
        .when(F.col("precip_mm") <= 7.6, "moderada")
        .otherwise("forte")
        .alias("faixa_chuva"),
    )
)
salvar(fato, "fato_oferta_linha_hora")

# COMMAND ----------

# MAGIC %md ## LGPD: tabela de apoio do MVP (Requisito 08)
# MAGIC **Dados pessoais:** a solução **não coleta** nome, CPF, e-mail, telefone nem localização de pessoas.
# MAGIC Os dados são de frota (ônibus) e de clima. Mesmo assim aplicamos minimização e proteção:
# MAGIC * o `prefixo` do veículo vira **hash SHA-256 truncado** (`veiculo_hash`); o original não chega à Ouro;
# MAGIC * latitude/longitude são **generalizadas** para 3 casas (~100 m);
# MAGIC * o fato é **agregado** por linha e hora, sem identificar veículo.
# MAGIC
# MAGIC **Contrato com o MVP:** o MVP consome `fato_oferta_linha_hora` + `dim_linha` (gráficos e veredito)
# MAGIC e `posicao_recente` (quantos ônibus e quantos acessíveis estão rodando agora), exportados em JSON no notebook 05.

# COMMAND ----------

ultimo = po.agg(F.max("ts_utc")).collect()[0][0]
janela = po.filter(F.col("ts_utc") >= F.lit(ultimo) - F.expr("INTERVAL 30 MINUTES"))
mais_recente = Window.partitionBy("prefixo").orderBy(F.col("ts_utc").desc())

posicao_recente = (
    janela.withColumn("_rn", F.row_number().over(mais_recente))
    .filter("_rn = 1")
    .select(
        F.substring(F.sha2(F.col("prefixo"), 256), 1, 12).alias("veiculo_hash"),  # HASH
        "cod_linha",
        "letreiro",
        "acessivel",
        F.round("lat", 3).alias("lat"),  # generalização ~100 m
        F.round("lon", 3).alias("lon"),
        "ts_local",
    )
)
salvar(posicao_recente, "posicao_recente")

# COMMAND ----------

# MAGIC %md ### Prova do hash (print para o relatório)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.prefixo AS original_na_prata, o.veiculo_hash AS na_ouro
# MAGIC FROM workspace.gs.prata_onibus p
# MAGIC JOIN workspace.gs.posicao_recente o
# MAGIC   ON substring(sha2(p.prefixo, 256), 1, 12) = o.veiculo_hash
# MAGIC LIMIT 5;
