# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup do ambiente (Unity Catalog)
# MAGIC
# MAGIC Cria a estrutura do Lakehouse no Unity Catalog:
# MAGIC
# MAGIC - **Catálogo** `mvp_co2_energia`: agrupa tudo o que pertence ao projeto;
# MAGIC - **Schemas** `bronze`, `silver` e `gold`: uma camada da arquitetura medalhão cada; e `governanca` para metadados operacionais;
# MAGIC - **Volume** `bronze.arquivos_brutos`: área de pouso (*landing zone*) dos arquivos originais, antes de virarem tabela.
# MAGIC
# MAGIC Todos os comandos usam `IF NOT EXISTS`, então o notebook é idempotente: pode ser executado várias vezes sem efeito colateral.

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

try:
    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {CATALOGO} "
        "COMMENT 'MVP Engenharia de Dados (PUC-Rio): emissões de CO2 e transição energética (Our World in Data)'"
    )
except Exception as erro:
    raise RuntimeError(
        f"Não foi possível criar o catálogo '{CATALOGO}'. Se o seu workspace não permite criar catálogos, "
        "edite o notebook 00_configuracao, troque CATALOGO para 'workspace' e execute este notebook novamente."
    ) from erro

spark.sql(f"USE CATALOG {CATALOGO}")

# COMMAND ----------

SCHEMAS = {
    SCHEMA_BRONZE: "Camada bronze: dados brutos exatamente como recebidos da fonte, com metadados de ingestão",
    SCHEMA_SILVER: "Camada silver: dados limpos, tipados, padronizados e com localidades classificadas",
    SCHEMA_GOLD: "Camada gold: modelo dimensional (constelação de fatos) pronto para análise",
    SCHEMA_GOVERNANCA: "Metadados operacionais do pipeline: log de ingestão, testes de qualidade e catálogo de dados",
}

for schema, descricao in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} COMMENT '{descricao}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA_BRONZE}.{VOLUME_BRUTO} "
    "COMMENT 'Landing zone: arquivos originais da Our World in Data (CSV e YAML), sem nenhuma alteração'"
)

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOGO}.{SCHEMA_BRONZE}"))
