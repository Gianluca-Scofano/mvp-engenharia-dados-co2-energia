# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração do projeto
# MAGIC
# MAGIC Parâmetros compartilhados por todos os notebooks do pipeline. Os demais notebooks executam este aqui com `%run ./00_configuracao`,
# MAGIC então nomes de catálogo, schemas, Volume e URLs das fontes ficam definidos em **um único lugar**.
# MAGIC
# MAGIC | Camada | Schema | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze` | Arquivos brutos (Volume) e tabelas Delta com o dado exatamente como veio, mais metadados de ingestão |
# MAGIC | Silver | `silver` | Dados limpos, tipados, padronizados e com as localidades classificadas |
# MAGIC | Gold | `gold` | Modelo dimensional (dimensões + fatos) pronto para responder às perguntas de negócio |
# MAGIC | Governança | `governanca` | Metadados operacionais: log de ingestão, resultados dos testes de qualidade e catálogo de dados |

# COMMAND ----------

# Catálogo do Unity Catalog onde o projeto vive.
# No Databricks Free Edition é possível criar catálogos. Se o seu workspace não permitir,
# troque para "workspace" (catálogo padrão que já vem criado) e rode o pipeline de novo.
CATALOGO = "mvp_co2_energia"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_GOVERNANCA = "governanca"

# Volume (landing zone da camada bronze) que recebe os arquivos exatamente como foram baixados
VOLUME_BRUTO = "arquivos_brutos"
CAMINHO_VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA_BRONZE}/{VOLUME_BRUTO}"

# Anos de referência das análises (definidos no diagnóstico de qualidade, notebook 03):
ANO_REFERENCIA = 2024   # último ano com emissões de CO2 para todos os países
ANO_PIB_FINAL = 2022    # último ano com PIB (Maddison Project Database)
POPULACAO_MINIMA_RANKING_PER_CAPITA = 1_000_000  # evita que microterritórios distorçam rankings per capita


def tabela(schema: str, nome: str) -> str:
    """Nome totalmente qualificado (catálogo.schema.tabela) de uma tabela do projeto."""
    return f"{CATALOGO}.{schema}.{nome}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fontes de dados
# MAGIC
# MAGIC As três fontes vêm dos repositórios **oficiais** da Our World in Data (OWID) no GitHub. As URLs apontam para um
# MAGIC **commit específico** (e não para o branch `master`), porque a OWID atualiza os arquivos periodicamente: fixar o commit
# MAGIC garante que qualquer pessoa que rodar o pipeline obtenha exatamente os mesmos dados e, portanto, os mesmos resultados.
# MAGIC O hash SHA-256 esperado de cada arquivo é conferido na ingestão.

# COMMAND ----------

COMMIT_CO2 = "382ee6c662b0ece26e111f263b44c029afad7787"
COMMIT_ENERGIA = "7e387a16f70a510e433f8aac7efeac6faa1e5059"
COMMIT_ETL = "32cf87e08ca01077e603c6383b81279eda7e8400"

_RAW = "https://raw.githubusercontent.com/owid"

FONTES = [
    {
        "arquivo": "owid-co2-data.csv",
        "tabela_bronze": "owid_co2_raw",
        "formato": "csv",
        "descricao": "Emissões anuais de CO2 e outros gases de efeito estufa por país/região (1750-2024)",
        "url": f"{_RAW}/co2-data/{COMMIT_CO2}/owid-co2-data.csv",
        "commit": COMMIT_CO2,
        "sha256": "7f78e2b218ce4bb8c538bbec04fdc9a7982e8d40bff972e650df603899edd5f6",
    },
    {
        "arquivo": "owid-energy-data.csv",
        "tabela_bronze": "owid_energia_raw",
        "formato": "csv",
        "descricao": "Consumo de energia primária e geração de eletricidade por fonte, país/região e ano (1900-2025)",
        "url": f"{_RAW}/energy-data/{COMMIT_ENERGIA}/owid-energy-data.csv",
        "commit": COMMIT_ENERGIA,
        "sha256": "266f2e2baad7975351bc9bb4aa061d22b1da9fe4c47d51d2ac6071e01e171f76",
    },
    {
        "arquivo": "regions.yml",
        "tabela_bronze": "owid_regioes_raw",
        "formato": "yaml",
        "descricao": "Definição oficial de regiões da OWID: códigos, países históricos e composição dos continentes",
        "url": f"{_RAW}/etl/{COMMIT_ETL}/etl/steps/data/garden/regions/2023-01-01/regions.yml",
        "commit": COMMIT_ETL,
        "sha256": "4a38164e30245da2ca800792c62d2ca02e61c824fe021fb6ef3777b4c3ac9003",
    },
    {
        "arquivo": "owid-co2-codebook.csv",
        "tabela_bronze": "owid_co2_codebook_raw",
        "formato": "csv",
        "descricao": "Dicionário de dados oficial do dataset de CO2 (descrição, unidade e fonte de cada coluna)",
        "url": f"{_RAW}/co2-data/{COMMIT_CO2}/owid-co2-codebook.csv",
        "commit": COMMIT_CO2,
        "sha256": "33b4f5e00efd58c7b83863f736beba1af4df946b43642b0400c3ec38648e0e8e",
    },
    {
        "arquivo": "owid-energy-codebook.csv",
        "tabela_bronze": "owid_energia_codebook_raw",
        "formato": "csv",
        "descricao": "Dicionário de dados oficial do dataset de energia (descrição, unidade e fonte de cada coluna)",
        "url": f"{_RAW}/energy-data/{COMMIT_ENERGIA}/owid-energy-codebook.csv",
        "commit": COMMIT_ENERGIA,
        "sha256": "3cc9b7db0d921496e2988568ce3aee5ed41f50431dd234a0663b5f0a4b2e32bb",
    },
]

print(f"Catálogo: {CATALOGO} | Volume: {CAMINHO_VOLUME} | {len(FONTES)} arquivos de origem configurados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro dos testes de qualidade
# MAGIC
# MAGIC Todas as verificações de qualidade (diagnóstico da bronze e testes pós-carga da silver e da gold) são gravadas na mesma
# MAGIC tabela `governanca.resultados_qualidade`, uma linha por verificação, com status `OK`, `ALERTA` (problema conhecido e tratado)
# MAGIC ou `FALHA`.

# COMMAND ----------

from datetime import datetime, timezone

ESQUEMA_RESULTADOS_QUALIDADE = (
    "data_execucao timestamp, etapa string, dimensao string, tabela string, verificacao string, "
    "valor_encontrado double, esperado string, status string, observacao string"
)


def resultado(dimensao, nome_tabela, verificacao, valor, esperado="0", status=None, observacao=""):
    """Monta o resultado de uma verificação. Sem status explícito: OK se o valor encontrado é zero, FALHA caso contrário."""
    valor = float(valor or 0)
    if status is None:
        status = "OK" if valor == 0 else "FALHA"
    return (dimensao, nome_tabela, verificacao, valor, esperado, status, observacao)


def salvar_resultados_qualidade(etapa, resultados, interromper_se_falhar=False):
    df = spark.createDataFrame(
        [(datetime.now(timezone.utc).replace(microsecond=0), etapa) + tuple(r) for r in resultados],
        ESQUEMA_RESULTADOS_QUALIDADE,
    )
    df.write.format("delta").mode("append").saveAsTable(tabela(SCHEMA_GOVERNANCA, "resultados_qualidade"))
    falhas = [r for r in resultados if r[5] == "FALHA"]
    if interromper_se_falhar and falhas:
        raise AssertionError(f"{len(falhas)} teste(s) de qualidade falharam: " + "; ".join(r[2] for r in falhas))
    return df
