# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Modelagem: camada Gold (modelo dimensional)
# MAGIC
# MAGIC A gold segue um **esquema em constelação** (*galaxy schema*): duas tabelas fato que compartilham dimensões conformadas.
# MAGIC Isso permite cruzar emissões e matriz energética do mesmo país no mesmo ano (*drill-across*), que é exatamente o que as
# MAGIC perguntas de negócio pedem.
# MAGIC
# MAGIC | Tabela | Tipo | Granularidade (uma linha por...) | Origem |
# MAGIC |---|---|---|---|
# MAGIC | `dim_localidade` | Dimensão | localidade (país, país histórico, continente, Mundo...) | `silver.localidades` |
# MAGIC | `dim_ano` | Dimensão | ano | anos presentes nas duas bases |
# MAGIC | `dim_fonte_energia` | Dimensão | fonte de energia | tabela de referência (classificação OWID: fóssil / baixo carbono) |
# MAGIC | `fato_emissoes_anual` | Fato | localidade × ano | `silver.emissoes_co2` |
# MAGIC | `fato_energia_fonte` | Fato | localidade × ano × fonte de energia | `silver.energia` (colunas por fonte transformadas em linhas) |
# MAGIC
# MAGIC **Por que manter Mundo e continentes na dimensão?** Porque as perguntas comparam países com o total mundial e entre continentes.
# MAGIC A dimensão traz `tipo_localidade` e `is_pais`: **rankings e somas usam sempre `is_pais = true`**, o que elimina a
# MAGIC dupla contagem encontrada no diagnóstico.
# MAGIC
# MAGIC A gold é inteiramente derivada da silver, por isso é reconstruída por completo a cada execução (*full refresh*).

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

DIM_LOCALIDADE = tabela(SCHEMA_GOLD, "dim_localidade")
DIM_ANO = tabela(SCHEMA_GOLD, "dim_ano")
DIM_FONTE = tabela(SCHEMA_GOLD, "dim_fonte_energia")
FATO_EMISSOES = tabela(SCHEMA_GOLD, "fato_emissoes_anual")
FATO_ENERGIA = tabela(SCHEMA_GOLD, "fato_energia_fonte")

# Fatos primeiro: eles referenciam as dimensões
for nome_tabela in [FATO_EMISSOES, FATO_ENERGIA, DIM_LOCALIDADE, DIM_ANO, DIM_FONTE]:
    spark.sql(f"DROP TABLE IF EXISTS {nome_tabela}")


# Chave primária de cada tabela: as colunas da chave são criadas como NOT NULL (o Delta rejeita nulos nelas)
CHAVES_PRIMARIAS = {
    DIM_LOCALIDADE: ["sk_localidade"],
    DIM_ANO: ["ano"],
    DIM_FONTE: ["sk_fonte"],
    FATO_EMISSOES: ["sk_localidade", "ano"],
    FATO_ENERGIA: ["sk_localidade", "ano", "sk_fonte"],
}


def salvar_gold(df, destino):
    """Cria a tabela via DDL (colunas da chave como NOT NULL) e carrega os dados."""
    chave = CHAVES_PRIMARIAS[destino]
    colunas = ", ".join(f"{c.name} {c.dataType.simpleString()}{' NOT NULL' if c.name in chave else ''}" for c in df.schema.fields)
    spark.sql(f"CREATE TABLE {destino} ({colunas}) USING DELTA")
    df.write.format("delta").mode("append").saveAsTable(destino)
    total = spark.table(destino).count()
    print(f"{destino}: {total:,} linhas")
    return spark.table(destino)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensões

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dim_localidade`
# MAGIC Chave substituta (`sk_localidade`) gerada de forma determinística (ordem alfabética do nome), para que as chaves sejam as
# MAGIC mesmas a cada reprocessamento.

# COMMAND ----------

dim_localidade = (spark.table(tabela(SCHEMA_SILVER, "localidades"))
                  .where("NOT descartada")
                  .withColumn("sk_localidade", F.row_number().over(Window.orderBy("nome_localidade")))
                  .select("sk_localidade", "codigo_localidade", "nome_localidade", "tipo_localidade", "is_pais",
                          "is_historico", "ano_fim_existencia", "continente", "presente_em_co2", "presente_em_energia"))
dim_localidade = salvar_gold(dim_localidade, DIM_LOCALIDADE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dim_ano`
# MAGIC Além de década e século, cada ano recebe o **período em relação aos acordos climáticos** (Protocolo de Kyoto, adotado em 1997,
# MAGIC e Acordo de Paris, adotado em 2015), útil para comparar tendências antes e depois de cada marco.

# COMMAND ----------

limites = (spark.table(tabela(SCHEMA_SILVER, "emissoes_co2")).select("ano")
           .unionByName(spark.table(tabela(SCHEMA_SILVER, "energia")).select("ano"))
           .agg(F.min("ano").alias("inicio"), F.max("ano").alias("fim")).first())

dim_ano = (spark.range(limites["inicio"], limites["fim"] + 1)
           .select(F.col("id").cast("int").alias("ano"))
           .withColumn("decada", (F.floor(F.col("ano") / 10) * 10).cast("int"))
           .withColumn("seculo", (F.floor((F.col("ano") - 1) / 100) + 1).cast("int"))
           .withColumn("periodo_acordos_climaticos",
                       F.when(F.col("ano") <= 1949, "1. Até 1949")
                        .when(F.col("ano") <= 1997, "2. 1950–1997 (antes de Kyoto)")
                        .when(F.col("ano") <= 2015, "3. 1998–2015 (Protocolo de Kyoto)")
                        .otherwise("4. 2016 em diante (Acordo de Paris)")))
dim_ano = salvar_gold(dim_ano, DIM_ANO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `dim_fonte_energia`
# MAGIC Tabela de referência com a mesma classificação usada pela OWID: **baixo carbono = renováveis + nuclear**;
# MAGIC **renováveis = hidrelétrica, solar, eólica, biocombustíveis/bioenergia e outras renováveis**.

# COMMAND ----------

dim_fonte = salvar_gold(spark.createDataFrame([
    (1, "carvao", "coal", "Carvão", "Fóssil", "Fóssil", False, False),
    (2, "petroleo", "oil", "Petróleo", "Fóssil", "Fóssil", False, False),
    (3, "gas_natural", "gas", "Gás natural", "Fóssil", "Fóssil", False, False),
    (4, "nuclear", "nuclear", "Nuclear", "Baixo carbono", "Nuclear", False, True),
    (5, "hidreletrica", "hydro", "Hidrelétrica", "Baixo carbono", "Renovável", True, True),
    (6, "solar", "solar", "Solar", "Baixo carbono", "Renovável", True, True),
    (7, "eolica", "wind", "Eólica", "Baixo carbono", "Renovável", True, True),
    (8, "biocombustiveis", "biofuel", "Biocombustíveis e bioenergia", "Baixo carbono", "Renovável", True, True),
    (9, "outras_renovaveis", "other_renewable", "Outras renováveis (geotérmica, marés...)", "Baixo carbono", "Renovável", True, True),
], "sk_fonte int, codigo_fonte string, codigo_fonte_owid string, nome_fonte string, categoria string, subcategoria string, "
   "is_renovavel boolean, is_baixo_carbono boolean"), DIM_FONTE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fatos

# COMMAND ----------

# MAGIC %md
# MAGIC ### `fato_emissoes_anual` (localidade × ano)

# COMMAND ----------

MEDIDAS_EMISSOES = [
    "populacao", "pib_usd_ppc_2011", "co2_mt", "co2_carvao_mt", "co2_petroleo_mt", "co2_gas_mt", "co2_cimento_mt",
    "co2_flaring_mt", "co2_outras_industrias_mt", "co2_uso_terra_mt", "co2_incl_uso_terra_mt", "co2_acumulado_mt",
    "co2_acumulado_incl_uso_terra_mt", "co2_per_capita_t", "co2_por_pib_kg_por_usd", "co2_consumo_mt",
    "co2_comercio_liquido_mt", "energia_primaria_twh", "energia_per_capita_kwh", "co2_por_energia_g_kwh",
    "participacao_co2_mundial_pct", "participacao_co2_acumulado_mundial_pct", "gee_total_mt_co2e", "metano_mt_co2e",
    "oxido_nitroso_mt_co2e",
]

fato_emissoes = (spark.table(tabela(SCHEMA_SILVER, "emissoes_co2"))
                 .join(dim_localidade.select("sk_localidade", "nome_localidade"), "nome_localidade", "inner")
                 .select("sk_localidade", "ano", *MEDIDAS_EMISSOES))
fato_emissoes = salvar_gold(fato_emissoes, FATO_EMISSOES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `fato_energia_fonte` (localidade × ano × fonte)
# MAGIC
# MAGIC A silver tem 4 colunas por fonte (36 colunas). Aqui elas viram **linhas** com a função `stack`, uma por fonte, o que
# MAGIC permite agrupar por categoria (fóssil / baixo carbono) via `dim_fonte_energia` em vez de somar colunas à mão.
# MAGIC
# MAGIC Regra de qualidade: a geração elétrica por fonte só é carregada quando a abertura por fonte **fecha com o total**
# MAGIC (`eletricidade_abertura_completa`, definida na silver). Caso contrário, as duas medidas de eletricidade ficam nulas.

# COMMAND ----------

codigos_fontes = [r["codigo_fonte"] for r in dim_fonte.orderBy("sk_fonte").collect()]
expressao_stack = "stack({n}, {valores}) AS (codigo_fonte, consumo_primario_twh, participacao_primaria_pct, geracao_eletrica_twh, participacao_eletrica_pct)".format(
    n=len(codigos_fontes),
    valores=", ".join(f"'{c}', consumo_{c}_twh, participacao_{c}_pct, eletricidade_{c}_twh, participacao_eletricidade_{c}_pct"
                      for c in codigos_fontes),
)

energia_longa = (spark.table(tabela(SCHEMA_SILVER, "energia"))
                 .select("nome_localidade", "ano", "eletricidade_abertura_completa", F.expr(expressao_stack))
                 .withColumn("geracao_eletrica_twh", F.when(F.col("eletricidade_abertura_completa"), F.col("geracao_eletrica_twh")))
                 .withColumn("participacao_eletrica_pct", F.when(F.col("eletricidade_abertura_completa"), F.col("participacao_eletrica_pct")))
                 .where("consumo_primario_twh IS NOT NULL OR participacao_primaria_pct IS NOT NULL "
                        "OR geracao_eletrica_twh IS NOT NULL OR participacao_eletrica_pct IS NOT NULL"))

fato_energia = (energia_longa
                .join(dim_localidade.select("sk_localidade", "nome_localidade"), "nome_localidade", "inner")
                .join(dim_fonte.select("sk_fonte", "codigo_fonte"), "codigo_fonte", "inner")
                .select("sk_localidade", "ano", "sk_fonte", "consumo_primario_twh", "participacao_primaria_pct",
                        "geracao_eletrica_twh", "participacao_eletrica_pct"))
fato_energia = salvar_gold(fato_energia, FATO_ENERGIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Restrições (chaves e regras de domínio)
# MAGIC
# MAGIC - **NOT NULL** nas colunas de chave: declarado no `CREATE TABLE` e garantido pelo Delta a cada escrita.
# MAGIC - **PRIMARY KEY / FOREIGN KEY** (Unity Catalog): documentam o modelo estrela no catálogo e habilitam o diagrama de
# MAGIC   entidade-relacionamento no Catalog Explorer. No Databricks elas são informativas (não bloqueiam escrita);
# MAGIC   por isso a integridade é **testada** na seção 4.
# MAGIC - **CHECK** (Delta Lake): são verificadas a cada escrita e impedem que dados fora do domínio entrem na gold em cargas futuras.

# COMMAND ----------

def executar(sql):
    try:
        spark.sql(sql)
        return "ok"
    except Exception as erro:  # PK/FK só existem no Unity Catalog
        return f"não aplicada ({type(erro).__name__})"


CHAVES_ESTRANGEIRAS = [
    (FATO_EMISSOES, "sk_localidade", DIM_LOCALIDADE),
    (FATO_EMISSOES, "ano", DIM_ANO),
    (FATO_ENERGIA, "sk_localidade", DIM_LOCALIDADE),
    (FATO_ENERGIA, "ano", DIM_ANO),
    (FATO_ENERGIA, "sk_fonte", DIM_FONTE),
]
CHECKS = [
    (DIM_ANO, "ck_ano_valido", "ano BETWEEN 1750 AND 2100"),
    (FATO_EMISSOES, "ck_co2_nao_negativo", "co2_mt IS NULL OR co2_mt >= 0"),
    (FATO_EMISSOES, "ck_populacao_positiva", "populacao IS NULL OR populacao > 0"),
    (FATO_EMISSOES, "ck_participacao_mundial", "participacao_co2_mundial_pct IS NULL OR participacao_co2_mundial_pct BETWEEN 0 AND 100"),
    (FATO_ENERGIA, "ck_participacao_primaria", "participacao_primaria_pct IS NULL OR participacao_primaria_pct BETWEEN 0 AND 100"),
    (FATO_ENERGIA, "ck_participacao_eletrica", "participacao_eletrica_pct IS NULL OR participacao_eletrica_pct BETWEEN 0 AND 100"),
    (FATO_ENERGIA, "ck_consumo_nao_negativo", "consumo_primario_twh IS NULL OR consumo_primario_twh >= 0"),
]

restricoes = []
for nome_tabela, colunas in CHAVES_PRIMARIAS.items():
    restricoes.append((nome_tabela, f"NOT NULL ({', '.join(colunas)})", "ok (declarada no CREATE TABLE)"))
    nome_pk = "pk_" + nome_tabela.split(".")[-1]
    restricoes.append((nome_tabela, f"PRIMARY KEY {nome_pk} ({', '.join(colunas)})",
                       executar(f"ALTER TABLE {nome_tabela} ADD CONSTRAINT {nome_pk} PRIMARY KEY ({', '.join(colunas)})")))
for nome_tabela, coluna, referencia in CHAVES_ESTRANGEIRAS:
    nome_fk = f"fk_{nome_tabela.split('.')[-1]}_{coluna}"
    restricoes.append((nome_tabela, f"FOREIGN KEY {nome_fk} ({coluna}) → {referencia.split('.')[-1]}",
                       executar(f"ALTER TABLE {nome_tabela} ADD CONSTRAINT {nome_fk} FOREIGN KEY ({coluna}) REFERENCES {referencia}")))
for nome_tabela, nome_check, condicao in CHECKS:
    restricoes.append((nome_tabela, f"CHECK {nome_check}: {condicao}",
                       executar(f"ALTER TABLE {nome_tabela} ADD CONSTRAINT {nome_check} CHECK ({condicao})")))

display(spark.createDataFrame(restricoes, "tabela string, restricao string, situacao string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Testes de qualidade da camada Gold
# MAGIC
# MAGIC Integridade do modelo (chaves únicas, chaves estrangeiras válidas) e **reconciliação** com os totais publicados pela OWID.
# MAGIC Qualquer falha interrompe o notebook.

# COMMAND ----------

def duplicadas(df, chave):
    return df.count() - df.select(*chave).distinct().count()


def orfas(fato, coluna, dimensao):
    return fato.join(dimensao, coluna, "left_anti").count()


# Reconciliação 1: países + transporte internacional = Mundo, ano a ano desde 1950
emissoes_com_tipo = fato_emissoes.join(dim_localidade.select("sk_localidade", "tipo_localidade"), "sk_localidade")
reconciliacao = (emissoes_com_tipo
                 .where("ano >= 1950")
                 .groupBy("ano")
                 .agg(F.sum(F.when(F.col("tipo_localidade").isin("País", "Transporte internacional"), F.col("co2_mt"))).alias("soma_partes"),
                      F.max(F.when(F.col("tipo_localidade") == "Mundo", F.col("co2_mt"))).alias("mundo"))
                 .withColumn("desvio_pct", F.abs(F.col("soma_partes") / F.col("mundo") - 1) * 100))
maior_desvio_mundo = reconciliacao.agg(F.max("desvio_pct")).first()[0]

# Reconciliação 2: soma das participações das fontes de baixo carbono (fato) = participação publicada (silver)
baixo_carbono_fato = (fato_energia.join(dim_fonte.where("is_baixo_carbono"), "sk_fonte")
                      .groupBy("sk_localidade", "ano")
                      .agg(F.sum("participacao_primaria_pct").alias("soma_fontes"), F.count("participacao_primaria_pct").alias("fontes")))
baixo_carbono_publicado = (spark.table(tabela(SCHEMA_SILVER, "energia"))
                           .join(dim_localidade.select("sk_localidade", "nome_localidade"), "nome_localidade")
                           .select("sk_localidade", "ano", "participacao_baixo_carbono_pct"))
maior_desvio_baixo_carbono = (baixo_carbono_fato.where("fontes = 6")
                              .join(baixo_carbono_publicado, ["sk_localidade", "ano"])
                              .agg(F.max(F.abs(F.col("soma_fontes") - F.col("participacao_baixo_carbono_pct")))).first()[0])

gold = lambda nome: f"{SCHEMA_GOLD}.{nome.split('.')[-1]}"
testes = [
    resultado("Unicidade", gold(DIM_LOCALIDADE), "sk_localidade duplicada", duplicadas(dim_localidade, ["sk_localidade"])),
    resultado("Unicidade", gold(DIM_LOCALIDADE), "nome_localidade duplicado", duplicadas(dim_localidade, ["nome_localidade"])),
    resultado("Unicidade", gold(DIM_ANO), "ano duplicado", duplicadas(dim_ano, ["ano"])),
    resultado("Unicidade", gold(DIM_FONTE), "sk_fonte duplicada", duplicadas(dim_fonte, ["sk_fonte"])),
    resultado("Unicidade", gold(FATO_EMISSOES), "chave (sk_localidade, ano) duplicada", duplicadas(fato_emissoes, ["sk_localidade", "ano"])),
    resultado("Unicidade", gold(FATO_ENERGIA), "chave (sk_localidade, ano, sk_fonte) duplicada",
              duplicadas(fato_energia, ["sk_localidade", "ano", "sk_fonte"])),
    resultado("Integridade", gold(FATO_EMISSOES), "sk_localidade sem correspondência na dimensão", orfas(fato_emissoes, "sk_localidade", dim_localidade)),
    resultado("Integridade", gold(FATO_EMISSOES), "ano sem correspondência na dimensão", orfas(fato_emissoes, "ano", dim_ano)),
    resultado("Integridade", gold(FATO_ENERGIA), "sk_localidade sem correspondência na dimensão", orfas(fato_energia, "sk_localidade", dim_localidade)),
    resultado("Integridade", gold(FATO_ENERGIA), "ano sem correspondência na dimensão", orfas(fato_energia, "ano", dim_ano)),
    resultado("Integridade", gold(FATO_ENERGIA), "sk_fonte sem correspondência na dimensão", orfas(fato_energia, "sk_fonte", dim_fonte)),
    resultado("Completude", gold(DIM_LOCALIDADE), "países sem continente", dim_localidade.where("is_pais AND continente IS NULL").count()),
    resultado("Consistência", gold(FATO_EMISSOES), "maior desvio (%) entre países + transporte internacional e o Mundo (1950+)",
              maior_desvio_mundo, "< 0,5%", "OK" if maior_desvio_mundo < 0.5 else "FALHA"),
    resultado("Consistência", gold(FATO_ENERGIA), "maior diferença (p.p.) entre soma das fontes de baixo carbono e o valor publicado",
              maior_desvio_baixo_carbono, "< 0,1 p.p.", "OK" if maior_desvio_baixo_carbono < 0.1 else "FALHA"),
]

df_testes = salvar_resultados_qualidade("05 gold", testes, interromper_se_falhar=True)
display(df_testes.select("dimensao", "tabela", "verificacao", "valor_encontrado", "esperado", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visão geral da gold

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.{SCHEMA_GOLD}"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT l.tipo_localidade, COUNT(DISTINCT l.sk_localidade) AS localidades, COUNT(*) AS linhas_fato_emissoes,
           MIN(f.ano) AS primeiro_ano, MAX(f.ano) AS ultimo_ano
    FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    GROUP BY l.tipo_localidade
    ORDER BY localidades DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Exemplo de consulta no modelo estrela: matriz de energia primária do Brasil em 2024 por categoria de fonte.

# COMMAND ----------

display(spark.sql(f"""
    SELECT fo.categoria, fo.nome_fonte,
           ROUND(fe.consumo_primario_twh, 1) AS consumo_twh,
           ROUND(fe.participacao_primaria_pct, 1) AS participacao_pct
    FROM {FATO_ENERGIA} fe
    JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    JOIN {DIM_FONTE} fo USING (sk_fonte)
    WHERE l.nome_localidade = 'Brazil' AND fe.ano = {ANO_REFERENCIA}
    ORDER BY fo.sk_fonte
"""))
