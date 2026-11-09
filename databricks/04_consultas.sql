-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As 5 consultas SQL da Ouro (Requisito 07)
-- MAGIC Cada consulta responde uma pergunta do MVP. Depois de rodar, **escreva a interpretação do resultado**
-- MAGIC no relatório (vale 2,5 pontos junto com a Ouro). A consulta 2 é a **consulta de impacto**.
-- MAGIC
-- MAGIC | # | Pergunta do cidadão / do gestor |
-- MAGIC |---|---|
-- MAGIC | 1 | Quais linhas têm mais ônibus nas ruas? |
-- MAGIC | 2 | **(impacto)** Quanto a oferta de ônibus muda quando chove? |
-- MAGIC | 3 | Em que período do dia há menos ônibus acessíveis? |
-- MAGIC | 4 | Quais linhas mais perdem ônibus quando chove? |
-- MAGIC | 5 | Como a oferta de ônibus varia ao longo do dia, e a chuva junto? |

-- COMMAND ----------

-- 1) Top 10 linhas com maior oferta média de ônibus por hora
SELECT l.letreiro,
       l.origem,
       l.destino,
       ROUND(AVG(f.qtd_onibus), 1) AS media_onibus_por_hora,
       COUNT(*)                    AS horas_observadas
FROM workspace.gs.fato_oferta_linha_hora f
JOIN workspace.gs.dim_linha l ON l.linha_id = f.linha_id
GROUP BY l.letreiro, l.origem, l.destino
ORDER BY media_onibus_por_hora DESC
LIMIT 10;

-- COMMAND ----------

-- 2) IMPACTO: variação percentual da oferta de ônibus por faixa de chuva, em relação às horas sem chuva
WITH por_faixa AS (
  SELECT faixa_chuva,
         COUNT(*)                    AS observacoes,
         ROUND(AVG(qtd_onibus), 2)   AS media_onibus
  FROM workspace.gs.fato_oferta_linha_hora
  WHERE faixa_chuva <> 'sem dado'
  GROUP BY faixa_chuva
),
referencia AS (
  SELECT media_onibus AS media_sem_chuva FROM por_faixa WHERE faixa_chuva = 'sem chuva'
)
SELECT p.faixa_chuva,
       p.observacoes,
       p.media_onibus,
       ROUND((p.media_onibus / r.media_sem_chuva - 1) * 100, 1) AS variacao_pct_vs_sem_chuva
FROM por_faixa p
CROSS JOIN referencia r
ORDER BY CASE p.faixa_chuva
           WHEN 'sem chuva' THEN 1 WHEN 'fraca' THEN 2
           WHEN 'moderada' THEN 3 ELSE 4 END;

-- COMMAND ----------

-- 3) Percentual de ônibus acessíveis por período do dia
SELECT t.periodo,
       SUM(f.qtd_onibus)                                           AS onibus_observados,
       SUM(f.qtd_acessiveis)                                       AS onibus_acessiveis,
       ROUND(SUM(f.qtd_acessiveis) / SUM(f.qtd_onibus) * 100, 1)   AS pct_acessivel
FROM workspace.gs.fato_oferta_linha_hora f
JOIN workspace.gs.dim_tempo t ON t.tempo_id = f.tempo_id
GROUP BY t.periodo
ORDER BY pct_acessivel ASC;

-- COMMAND ----------

-- 4) Linhas que mais perdem ônibus nas horas com chuva (só linhas observadas com e sem chuva)
SELECT l.letreiro,
       l.destino,
       ROUND(AVG(CASE WHEN f.faixa_chuva = 'sem chuva' THEN f.qtd_onibus END), 1) AS media_sem_chuva,
       ROUND(AVG(CASE WHEN f.faixa_chuva IN ('fraca','moderada','forte') THEN f.qtd_onibus END), 1) AS media_com_chuva,
       ROUND((AVG(CASE WHEN f.faixa_chuva IN ('fraca','moderada','forte') THEN f.qtd_onibus END)
            / AVG(CASE WHEN f.faixa_chuva = 'sem chuva' THEN f.qtd_onibus END) - 1) * 100, 1) AS variacao_pct
FROM workspace.gs.fato_oferta_linha_hora f
JOIN workspace.gs.dim_linha l ON l.linha_id = f.linha_id
GROUP BY l.letreiro, l.destino
HAVING COUNT(CASE WHEN f.faixa_chuva = 'sem chuva' THEN 1 END) >= 2
   AND COUNT(CASE WHEN f.faixa_chuva IN ('fraca','moderada','forte') THEN 1 END) >= 2
ORDER BY variacao_pct ASC
LIMIT 10;

-- COMMAND ----------

-- 5) Curva da oferta ao longo do dia, com a chuva média de cada hora
SELECT t.hora_do_dia,
       ROUND(AVG(f.qtd_onibus), 1)  AS media_onibus_por_linha,
       ROUND(AVG(f.precip_mm), 2)   AS chuva_media_mm,
       COUNT(DISTINCT t.data)       AS dias_observados
FROM workspace.gs.fato_oferta_linha_hora f
JOIN workspace.gs.dim_tempo t ON t.tempo_id = f.tempo_id
GROUP BY t.hora_do_dia
ORDER BY t.hora_do_dia;
