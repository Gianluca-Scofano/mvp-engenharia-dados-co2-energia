# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Coleta e camada Bronze
# MAGIC
# MAGIC **Objetivo:** trazer os arquivos da Our World in Data para a nuvem e registrá-los como tabelas Delta **sem nenhuma alteração de conteúdo**.
# MAGIC
# MAGIC Fluxo de cada arquivo:
# MAGIC
# MAGIC 1. **Coleta** → se o arquivo ainda não está no Volume `bronze.arquivos_brutos`, ele é baixado da URL oficial (fixada em um commit) e copiado para lá.
# MAGIC    Se o ambiente não tiver acesso à internet, basta fazer o upload manual do arquivo para o Volume pela interface e reexecutar: o notebook reaproveita o que já está lá.
# MAGIC 2. **Verificação de integridade** → o hash SHA-256 do arquivo é comparado com o hash esperado da versão fixada.
# MAGIC 3. **Carga Bronze** → o arquivo vira uma tabela Delta com **todas as colunas como texto** (nada é reinterpretado na entrada) mais colunas de controle:
# MAGIC    `_arquivo_origem`, `_url_origem`, `_versao_origem` e `_data_ingestao`.
# MAGIC 4. **Log** → cada ingestão é registrada em `governanca.controle_ingestao` (arquivo, hash, tamanho, modo de coleta, linhas carregadas).
# MAGIC
# MAGIC Guardar o bruto como texto preserva a rastreabilidade: se algo der errado nas camadas seguintes, sempre é possível voltar ao bronze e ver exatamente o que chegou.

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

import hashlib
import os
import shutil
import tempfile
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import functions as F

# Um único instante para toda a execução: todas as tabelas e o log compartilham a mesma data de ingestão
DATA_INGESTAO = datetime.now(timezone.utc).replace(microsecond=0)


def calcular_sha256(caminho: str) -> str:
    h = hashlib.sha256()
    with open(caminho, "rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest()


def garantir_arquivo_no_volume(fonte: dict) -> tuple:
    """Garante que o arquivo está no Volume. Retorna (caminho, modo_de_coleta)."""
    destino = f"{CAMINHO_VOLUME}/{fonte['arquivo']}"
    if os.path.exists(destino):
        return destino, "reaproveitado do Volume (upload manual ou execução anterior)"

    temporario = os.path.join(tempfile.mkdtemp(), fonte["arquivo"])
    try:
        urllib.request.urlretrieve(fonte["url"], temporario)
    except Exception as erro:
        raise RuntimeError(
            f"Falha ao baixar {fonte['url']}: {erro}\n"
            f"Faça o upload manual de '{fonte['arquivo']}' para o Volume {CAMINHO_VOLUME} "
            f"(Catalog > {CATALOGO} > {SCHEMA_BRONZE} > {VOLUME_BRUTO} > Upload to this volume) e execute o notebook novamente."
        ) from erro
    shutil.copyfile(temporario, destino)
    return destino, "download automático da URL oficial"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Coleta dos arquivos para o Volume

# COMMAND ----------

arquivos = []
for fonte in FONTES:
    caminho, modo = garantir_arquivo_no_volume(fonte)
    sha = calcular_sha256(caminho)
    arquivos.append({**fonte, "caminho": caminho, "modo_coleta": modo, "sha256_calculado": sha,
                     "tamanho_bytes": os.path.getsize(caminho)})
    situacao = "confere com a versão fixada" if sha == fonte["sha256"] else "DIFERENTE da versão fixada: os resultados podem divergir do README"
    print(f"{fonte['arquivo']:<28} {os.path.getsize(caminho)/1e6:7.2f} MB | {modo} | SHA-256 {situacao}")

# COMMAND ----------

display(spark.createDataFrame(
    [(a["arquivo"], a["tamanho_bytes"], a["modo_coleta"], a["caminho"]) for a in arquivos],
    "arquivo string, tamanho_bytes long, modo_coleta string, caminho_no_volume string",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga dos arquivos CSV como tabelas Delta (bronze)
# MAGIC
# MAGIC `inferSchema = false`: toda coluna entra como `STRING`. A tipagem é responsabilidade da camada silver, onde cada conversão
# MAGIC é feita de forma explícita e verificada.

# COMMAND ----------

def colunas_de_controle(df, fonte):
    return (df
            .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
            .withColumn("_url_origem", F.lit(fonte["url"]))
            .withColumn("_versao_origem", F.lit(fonte["commit"]))
            .withColumn("_data_ingestao", F.lit(DATA_INGESTAO)))


def salvar_bronze(df, nome_tabela: str) -> int:
    destino = tabela(SCHEMA_BRONZE, nome_tabela)
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(destino))
    return spark.table(destino).count()


linhas_carregadas = {}
for a in arquivos:
    if a["formato"] != "csv":
        continue
    df_csv = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "false")
              .option("encoding", "UTF-8")
              .load(a["caminho"]))
    linhas_carregadas[a["arquivo"]] = salvar_bronze(colunas_de_controle(df_csv, a), a["tabela_bronze"])
    print(f"{tabela(SCHEMA_BRONZE, a['tabela_bronze'])}: {linhas_carregadas[a['arquivo']]:,} linhas, {len(df_csv.columns)} colunas de origem")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carga do arquivo YAML de regiões (bronze)
# MAGIC
# MAGIC O `regions.yml` é a definição oficial de regiões usada pela própria OWID para calcular os agregados (continentes, Mundo etc.).
# MAGIC Ele é convertido de YAML para tabela **sem interpretar valores**: campos simples viram texto e listas viram `ARRAY<STRING>`.

# COMMAND ----------

try:
    import yaml
except ImportError:  # o PyYAML costuma vir instalado no Databricks; se não vier, instala na sessão
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "pyyaml"])
    import yaml


def como_texto(valor):
    if valor is None:
        return None
    if isinstance(valor, bool):
        return str(valor).lower()
    return str(valor)


def como_lista(valor):
    return None if valor is None else [str(item) for item in valor]


fonte_regioes = next(a for a in arquivos if a["formato"] == "yaml")
with open(fonte_regioes["caminho"], encoding="utf-8") as arquivo:
    regioes = yaml.safe_load(arquivo)

CAMPOS_TEXTO = ["code", "name", "short_name", "region_type", "defined_by", "is_historical", "end_year"]
CAMPOS_LISTA = ["members", "aliases", "successors", "related"]

df_regioes = spark.createDataFrame(
    [tuple(como_texto(r.get(c)) for c in CAMPOS_TEXTO) + tuple(como_lista(r.get(c)) for c in CAMPOS_LISTA) for r in regioes],
    ", ".join([f"{c} string" for c in CAMPOS_TEXTO] + [f"{c} array<string>" for c in CAMPOS_LISTA]),
)
df_regioes = (df_regioes
              .withColumn("_arquivo_origem", F.lit(fonte_regioes["caminho"]))
              .withColumn("_url_origem", F.lit(fonte_regioes["url"]))
              .withColumn("_versao_origem", F.lit(fonte_regioes["commit"]))
              .withColumn("_data_ingestao", F.lit(DATA_INGESTAO)))

linhas_carregadas[fonte_regioes["arquivo"]] = salvar_bronze(df_regioes, fonte_regioes["tabela_bronze"])
print(f"{tabela(SCHEMA_BRONZE, fonte_regioes['tabela_bronze'])}: {linhas_carregadas[fonte_regioes['arquivo']]:,} regiões")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log de ingestão (`governanca.controle_ingestao`)
# MAGIC
# MAGIC Tabela de auditoria em modo *append*: cada execução acrescenta uma linha por arquivo, permitindo saber quando, de onde,
# MAGIC em qual versão e em que volume cada dado entrou no Lakehouse.

# COMMAND ----------

df_log = spark.createDataFrame(
    [(DATA_INGESTAO, a["arquivo"], a["url"], a["commit"], a["sha256_calculado"], a["sha256_calculado"] == a["sha256"],
      a["tamanho_bytes"], a["modo_coleta"], tabela(SCHEMA_BRONZE, a["tabela_bronze"]), linhas_carregadas[a["arquivo"]])
     for a in arquivos],
    "data_ingestao timestamp, arquivo string, url_origem string, versao_origem string, sha256 string, "
    "sha256_confere boolean, tamanho_bytes long, modo_coleta string, tabela_destino string, linhas_carregadas long",
)
df_log.write.format("delta").mode("append").saveAsTable(tabela(SCHEMA_GOVERNANCA, "controle_ingestao"))

display(spark.table(tabela(SCHEMA_GOVERNANCA, "controle_ingestao")).orderBy(F.desc("data_ingestao"), "arquivo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência rápida do que chegou

# COMMAND ----------

display(spark.table(tabela(SCHEMA_BRONZE, "owid_co2_raw"))
        .select("country", "year", "iso_code", "population", "co2", "co2_per_capita", "_versao_origem", "_data_ingestao")
        .where("country IN ('Brazil', 'World') AND year >= '2020'")
        .orderBy("country", "year"))

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.{SCHEMA_BRONZE}"))
