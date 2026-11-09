# Guia passo a passo · Big Data (Ônibus & Chuva SP)

Prazo: **19/11 às 23h55** no Teams da disciplina. Só um integrante envia.
Hoje é 08/11 (domingo). **O que decide o cronograma é a coleta:** a API da SPTrans só mostra o "agora", então os 2 lotes de dias diferentes só existem se o Airflow rodar em 2 dias reais. **Comece a coletar hoje ou amanhã.**

## Cronograma sugerido

| Quando | O que fazer |
|---|---|
| 08 e 09/11 | Token SPTrans, Airflow no ar, 1ª coleta. Criar repositório. |
| 09 a 11/11 | Airflow coletando. Fazer o 1º upload (lote 1) e rodar Bronze e Prata com ele. |
| 11 a 13/11 | Lote 2 (outro dia). Rodar Bronze de novo e provar que só entram arquivos novos. |
| 13 a 15/11 | Ouro, 5 consultas SQL, LGPD, exportar `ouro.json`. |
| 15 a 17/11 | MVP publicado (GitHub Pages) com dados reais. |
| 17 a 18/11 | Relatório em PDF e revisão do repositório em janela anônima. |
| 19/11 | Envio no Teams, de preferência de manhã (o dia 20 é feriado). |

---

## Etapa 0 · Repositório

1. Crie um repositório **público** no GitHub e suba esta pasta (já tem a estrutura pedida: `dags/`, `databricks/`, `amostras/`, `README.md`).
2. Preencha **integrantes (nome completo e RM)** no README. Falta de nome ou RM tira 0,5 ponto.
3. Confira que o `.gitignore` está no repositório e que nenhum token foi commitado.

## Requisito 01 · Fontes de dados

1. **SPTrans Olho Vivo:** gere o token gratuito no portal de desenvolvedores da SPTrans (o slide da disciplina indica "token gratuito"). Guarde o token sem colocar em arquivo do repositório.
2. **Open-Meteo:** não precisa de chave.
3. Teste as duas à mão antes de codar:
   - Open-Meteo: abra no navegador `https://api.open-meteo.com/v1/forecast?latitude=-23.55&longitude=-46.63&hourly=temperature_2m,precipitation&timezone=America/Sao_Paulo`
   - SPTrans: no Postman, `POST http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=SEU_TOKEN` (deve responder `true`) e depois `GET .../v2.1/Posicao` na mesma sessão (usa cookie).
4. Print das duas respostas para o relatório.

## Requisitos 02 e 03 · DAG no Airflow 3

1. Suba o Airflow 3 com o **Docker Compose da documentação oficial do Airflow 3** e adicione ao serviço do Airflow o volume `./chegada:/opt/airflow/chegada`.
2. Copie `dags/gs_coleta.py` para a pasta `dags/` do Airflow.
3. Em **Admin > Variables**, crie `SPTRANS_TOKEN` (obrigatória). Opcional: `ALERTA_WEBHOOK` (notificação de falha).
4. Ative o DAG `gs_coleta` e clique em **Trigger** para a primeira coleta.
5. Confira em `chegada/onibus/` e `chegada/clima/` os arquivos `onibus_AAAA-MM-DD_HHh.json` e `clima_AAAA-MM-DD_HHh.json`.
6. **Prints do relatório:** visão **Graph** (inicio → 2 coletas em paralelo → conferir_chegada) e visão **Grid** com execuções concluídas em verdes, em mais de um dia.
7. A tarefa `conferir_chegada` já falha se um arquivo faltar ou vier vazio. Mostre também um print de uma falha tratada, se acontecer (ganha ponto de resiliência).

> Se o problema for outra cidade, troque `LATITUDE`/`LONGITUDE` no DAG e os limites de coordenada na regra 2 da Prata.

## Requisito 04 · Dois lotes no Volume do Databricks

1. Crie a conta no **Databricks Free Edition**.
2. Importe os notebooks de `databricks/` (Workspace > Import).
3. Rode `00_setup.sql`. Depois, em **Catalog > workspace > gs > chegada**, crie as pastas `onibus` e `clima`.
4. **Lote 1:** envie os arquivos do 1º dia de coleta com **Upload to this volume** em cada pasta.
5. **Lote 2:** só depois, num dia de coleta diferente, envie os arquivos novos.
6. Print do Volume mostrando os arquivos dos dois dias.

## Requisito 05 · Bronze incremental

1. Depois do **lote 1**, rode `01_bronze.py`. Tire print das células "ANTES" e "DEPOIS".
2. Envie o **lote 2** e rode de novo. O print deve mostrar que só os arquivos novos entraram ("novos arquivos lidos").
3. Rode **uma terceira vez sem enviar nada**. O resultado deve ser 0 novos e contagens iguais. Esse print prova que não duplica (vale 1,5 ponto).

## Requisito 06 · Prata

1. Rode `02_prata.py` depois de cada lote.
2. Mostre no relatório:
   - os tipos corrigidos (`DESCRIBE TABLE workspace.gs.prata_onibus`);
   - o tratamento de nulos (letreiro nulo → "N/D", acessível nulo → falso);
   - o `MERGE` e a contagem antes e depois (rodar de novo não muda o total);
   - a tabela `prata_qualidade` com o total barrado por cada uma das 3 regras.
3. Se nenhuma regra barrar nada, tudo bem, mas **explique** por que (o dado já vinha limpo) e mostre `prata_onibus_rejeitados`.

## Requisito 07 · Ouro e 5 consultas SQL

1. Rode `03_ouro.py`. Print das tabelas `fato_oferta_linha_hora`, `dim_linha` e `dim_tempo`, e o diagrama estrela (está no README).
2. Rode `04_consultas.sql`. Para **cada** uma das 5 consultas, o relatório precisa de: a consulta, o resultado e **a sua interpretação em 2 ou 3 frases**.
3. A consulta 2 é a de **impacto**. Interprete com honestidade: com poucos dias de dados e sem chuva forte, o efeito pode não aparecer, e isso também é um resultado válido.

## Requisito 08 · LGPD e uso pelo MVP

1. Rode a célula de prova do hash no final de `03_ouro.py` e tire o print (prefixo original na Prata × hash na Ouro).
2. Explique no relatório: não há dados pessoais; mesmo assim aplicamos hash, generalização de coordenadas e agregação.
3. Rode `05_exportar_para_mvp.py`, baixe `ouro.json` em **Catalog > workspace > gs > chegada > _export** e substitua `mvp/data/ouro.json`.
4. Mostre qual tabela da Ouro o MVP usa e a **tela** do MVP exibindo esses dados.

## MVP publicado (alimenta o requisito 08)

1. Teste local: `cd mvp && python -m http.server 8000`.
2. Publique no **GitHub Pages**: *Settings > Pages > Deploy from a branch*, pasta `/mvp` (se a opção não aparecer, mova o conteúdo de `mvp/` para `docs/`).
3. Confira que a faixa de aviso "dados de exemplo (simulados)" **sumiu**. Ela só aparece quando `"simulado": true` no JSON. Entregar dados simulados como se fossem reais seria um problema.
4. Combine com o grupo: quem cuida do deploy (DevOps) pode hospedar o mesmo `mvp/` na nuvem.

## Requisito 09 · README e repositório

1. Complete o README: integrantes, Ficha Técnica (URL do MVP e do repositório), arquitetura e passo a passo (já está escrito, só ajuste).
2. Na pasta `amostras/`, **troque os arquivos `_EXEMPLO_`** por 1 ou 2 arquivos reais coletados (corte-os se forem grandes).
3. Garanta: sem tokens, sem senhas, sem dados pessoais reais (−1,0 se houver).

## Relatório em PDF (nesta ordem)

1. Nome completo e RM dos integrantes
2. Produto em uma frase e as perguntas que o MVP responde (estão no README)
3. Diagrama do fluxo de dados, da fonte ao MVP (o diagrama do README)
4. Tabela de fontes: origem, formato e frequência de coleta

   | Fonte | Origem | Formato | Frequência |
   |---|---|---|---|
   | Posição dos ônibus | SPTrans Olho Vivo (API) | JSON | 3x ao dia |
   | Chuva e temperatura | Open-Meteo (API) | JSON | 3x ao dia |
5. Requisitos 01 a 09, na ordem, cada um com print e explicação curta
6. Prints do Airflow: Graph e Grid com execuções concluídas
7. Contagens da Bronze antes e depois do 2º lote e da 2ª execução
8. As 5 consultas SQL com resultado e interpretação
9. Conclusão técnica e link do repositório

Prints **legíveis e com legenda**: prints ilegíveis, sem legenda ou fora da ordem tiram até 1,0 ponto.

## Checklist final (antes de enviar)

- [ ] Repositório público, aberto numa janela anônima
- [ ] Nome completo e RM de todos no relatório e no README
- [ ] Nenhum token, senha ou dado pessoal no repositório
- [ ] DAG com 2 tarefas em paralelo e tarefa de conferência
- [ ] 2 lotes de dias diferentes no Volume
- [ ] Prints da Bronze: antes, depois e 2ª execução sem novidade
- [ ] `prata_qualidade` com as 3 regras e totais
- [ ] 5 consultas com interpretação (1 de impacto)
- [ ] Hash na Ouro e tela do MVP com dados reais
- [ ] PDF enviado por **um** integrante, com o link do repositório

## Armadilhas

- **Não entregue o exemplo sem adaptar:** o enunciado diz que os itens 01 e 07 valem zero nesse caso. Este projeto já está adaptado ao problema "ônibus × chuva", mas ajuste para o recorte que o grupo escolher.
- **Dados simulados:** os arquivos `_EXEMPLO_` e o `ouro.json` inicial são simulados só para você testar. Troque-os pelos reais antes da entrega.
- **Repositório privado, vazio ou fora do ar durante a correção** zera os itens que dependem dele. A correção ocorre em até uma semana depois do prazo.
- **Sem chuva no período:** se não chover, as consultas 2 e 4 não terão comparação. Explique no relatório e use os dados de chuva fraca como apoio. Novembro costuma ter chuva em SP, então colete o máximo de dias possível.
