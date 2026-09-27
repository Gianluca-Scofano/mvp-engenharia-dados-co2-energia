# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Executar o pipeline completo
# MAGIC
# MAGIC Orquestra todo o pipeline com um único **Run all**: executa os notebooks `01` a `07` em sequência com `dbutils.notebook.run`.
# MAGIC Se uma etapa falhar, as seguintes não são executadas, porque cada camada depende da anterior.
# MAGIC
# MAGIC Cada etapa roda como uma execução separada. Clique no link **"Notebook job #..."** que aparece abaixo da célula para abrir a
# MAGIC etapa com todas as saídas: tabelas, gráficos e testes.

# COMMAND ----------

import time

ETAPAS = [
    ("01_setup_ambiente", "Setup do ambiente (catálogo, schemas e Volume)"),
    ("02_coleta_bronze", "Coleta e camada bronze"),
    ("03_qualidade_diagnostico", "Diagnóstico de qualidade (bronze)"),
    ("04_transformacao_silver", "Transformação: camada silver"),
    ("05_modelagem_gold", "Modelagem: camada gold"),
    ("06_catalogo_dados", "Catálogo de dados (Unity Catalog)"),
    ("07_analise", "Análise: perguntas de negócio"),
]
TEMPO_LIMITE_POR_ETAPA_S = 3600

execucao = []
for notebook, descricao in ETAPAS:
    inicio = time.time()
    try:
        dbutils.notebook.run(f"./{notebook}", TEMPO_LIMITE_POR_ETAPA_S)
        status = "OK"
    except Exception as erro:
        status = f"FALHOU: {str(erro)[:300]}"
    duracao = round(time.time() - inicio, 1)
    execucao.append((len(execucao) + 1, notebook, descricao, status, duracao))
    print(f"{notebook:<28} {status[:60]:<60} {duracao:>7.1f}s")
    if status != "OK":
        print("Pipeline interrompido: corrija a etapa que falhou e execute novamente.")
        break

# COMMAND ----------

display(spark.createDataFrame(execucao, "ordem int, notebook string, etapa string, status string, duracao_s double"))

# COMMAND ----------

falhas = [e for e in execucao if e[3] != "OK"]
assert not falhas and len(execucao) == len(ETAPAS), f"Pipeline incompleto: {falhas}"
print(f"Pipeline completo: {len(execucao)} etapas executadas com sucesso em {sum(e[4] for e in execucao):.0f} segundos.")
