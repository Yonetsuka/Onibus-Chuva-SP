-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup (rodar uma vez)
-- MAGIC Cria o schema `workspace.gs` e o Volume `chegada`, onde os arquivos coletados pelo Airflow são enviados.
-- MAGIC
-- MAGIC Depois de rodar, abra **Catalog > workspace > gs > chegada** e crie as pastas `onibus` e `clima`.
-- MAGIC Em cada lote, use **Upload to this volume** e envie os arquivos da pasta `chegada/onibus` e `chegada/clima`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.gs;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.gs.chegada;

-- COMMAND ----------

-- Conferência: deve listar as pastas que você criou (onibus, clima)
LIST '/Volumes/workspace/gs/chegada/';
