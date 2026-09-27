# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Transformação: camada Silver
# MAGIC
# MAGIC Aplica os tratamentos decididos no diagnóstico de qualidade (notebook 03). Cada etapa registra quantas linhas entraram e
# MAGIC quantas saíram, para que o impacto de cada transformação fique documentado.
# MAGIC
# MAGIC | # | Problema encontrado na bronze | Tratamento na silver |
# MAGIC |---|---|---|
# MAGIC | 1 | Tudo chega como texto | Conversão explícita e tolerante (`try_cast`) para `INT`, `BIGINT` e `DOUBLE` |
# MAGIC | 2 | Nomes de colunas em inglês e sem unidade | Renomeação para português com a unidade no nome (`co2_mt`, `consumo_carvao_twh`...) |
# MAGIC | 3 | Países misturados com Mundo, continentes, grupos de renda e agregados de fontes | Tabela `silver.localidades` classifica cada localidade em um `tipo_localidade` |
# MAGIC | 4 | 90+ agregados definidos por fontes específicas (`Europe (EI)`, `Africa (GCP)`...), redundantes e sobrepostos | Descartados (continuam na bronze para rastreabilidade) |
# MAGIC | 5 | Kosovo e países históricos (URSS, Iugoslávia...) sem código ISO | Código e continente obtidos da definição oficial de regiões da OWID (`regions.yml`) |
# MAGIC | 6 | Nenhuma informação de continente nas bases | Continente de cada país derivado da composição oficial dos continentes da OWID |
# MAGIC | 7 | Linhas sem nenhuma medida (só população/PIB, anos muito antigos) | Removidas das tabelas de fatos da silver |
# MAGIC | 8 | Abertura da eletricidade por fonte incompleta em anos antigos | Flag `eletricidade_abertura_completa` (soma das fontes = total ± 1%) usada na gold |
# MAGIC | 9 | Bioenergia às vezes vem separada, às vezes embutida em "outras renováveis" | Regra única: `outras_renovaveis` = outras renováveis **sem** bioenergia quando a abertura existe |
# MAGIC | 10 | Duplicatas (não encontradas, mas possíveis em recargas futuras) | Deduplicação defensiva pela chave (localidade, ano) |

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

log_transformacoes = []  # (tabela, etapa, linhas_antes, linhas_depois, descricao)


def registrar_etapa(tabela_silver, etapa, antes, depois, descricao):
    log_transformacoes.append((tabela_silver, etapa, int(antes), int(depois), descricao))
    print(f"[{tabela_silver}] {etapa}: {antes:,} → {depois:,} linhas")


def salvar_silver(df, nome_tabela):
    destino = tabela(SCHEMA_SILVER, nome_tabela)
    (df.withColumn("_data_processamento", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino))
    return spark.table(destino)


def num(coluna):
    return F.expr(f"try_cast(`{coluna}` AS DOUBLE)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `silver.regioes_owid`: referência oficial de regiões
# MAGIC
# MAGIC Do `regions.yml` aproveitamos: código de cada região, se é histórica (e até quando existiu), nomes alternativos e a
# MAGIC **composição de cada continente** (lista de membros), que é "explodida" para descobrir o continente de cada país.

# COMMAND ----------

CONTINENTES_PT = {"Africa": "África", "Asia": "Ásia", "Europe": "Europa", "North America": "América do Norte",
                  "South America": "América do Sul", "Oceania": "Oceania"}
mapa_continentes = F.create_map(*[F.lit(v) for par in CONTINENTES_PT.items() for v in par])

regioes_raw = spark.table(tabela(SCHEMA_BRONZE, "owid_regioes_raw"))
regioes = regioes_raw.select(
    F.col("code").alias("codigo_regiao"),
    F.col("name").alias("nome_regiao"),
    F.coalesce(F.col("region_type"), F.lit("country")).alias("tipo_regiao_owid"),
    F.coalesce(F.col("is_historical") == "true", F.lit(False)).alias("is_historico"),
    F.expr("try_cast(end_year AS INT)").alias("ano_fim_existencia"),
    F.coalesce(F.col("aliases"), F.array().cast("array<string>")).alias("nomes_alternativos"),
    F.coalesce(F.col("members"), F.array().cast("array<string>")).alias("membros"),
)

membros_continentes = (regioes.where("tipo_regiao_owid = 'continent'")
                       .select(mapa_continentes[F.col("nome_regiao")].alias("continente"),
                               F.explode("membros").alias("codigo_regiao")))

regioes_silver = (regioes
                  .join(membros_continentes, "codigo_regiao", "left")
                  .withColumn("continente", F.when(F.col("tipo_regiao_owid") == "continent", mapa_continentes[F.col("nome_regiao")])
                                             .otherwise(F.col("continente")))
                  .drop("membros"))

regioes_silver = salvar_silver(regioes_silver, "regioes_owid")
registrar_etapa("regioes_owid", "YAML → tabela tipada + continente de cada membro", regioes_raw.count(), regioes_silver.count(),
                "tipos corrigidos (booleano/inteiro); continente derivado da lista de membros de cada continente")
display(regioes_silver.where("codigo_regiao IN ('BRA', 'USA', 'CHN', 'OWID_USS', 'OWID_KOS', 'OWID_AFR')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver.localidades`: classificação de cada localidade
# MAGIC
# MAGIC Reúne todas as localidades que aparecem nas duas bases e define o `tipo_localidade`:
# MAGIC
# MAGIC | Tipo | Regra | Exemplos |
# MAGIC |---|---|---|
# MAGIC | País histórico | marcado como histórico no `regions.yml` (mesmo que tenha código ISO) | USSR, Yugoslavia, Netherlands Antilles |
# MAGIC | País | tem código ISO de 3 letras, ou é tratado como país pela OWID (Kosovo) | Brazil, China, Kosovo |
# MAGIC | Mundo | `World` | World |
# MAGIC | Continente | os 6 continentes definidos pela OWID | Africa, South America |
# MAGIC | Grupo de renda | classificação do Banco Mundial | High-income countries |
# MAGIC | Bloco econômico | União Europeia com 27 membros | European Union (27) |
# MAGIC | Transporte internacional | emissões de aviação e navegação internacionais, não atribuídas a nenhum país | International shipping |
# MAGIC | Outro | eventos/territórios especiais mantidos por completude | Kuwaiti Oil Fires, Ryukyu Islands |
# MAGIC | Agregado de fonte específica → **descartado** | todas as demais localidades sem código ISO | Europe (EI), Africa (GCP), OECD (Ember), Europe (excl. EU-27) |

# COMMAND ----------

MUNDO = ["World"]
CONTINENTES = list(CONTINENTES_PT)
GRUPOS_RENDA = ["High-income countries", "Upper-middle-income countries", "Lower-middle-income countries", "Low-income countries"]
BLOCOS_ECONOMICOS = ["European Union (27)"]
TRANSPORTE_INTERNACIONAL = ["International aviation", "International shipping"]
OUTROS = ["Kuwaiti Oil Fires", "Ryukyu Islands"]

co2_raw = spark.table(tabela(SCHEMA_BRONZE, "owid_co2_raw"))
energia_raw = spark.table(tabela(SCHEMA_BRONZE, "owid_energia_raw"))

localidades_brutas = (
    co2_raw.select(F.col("country").alias("nome_localidade"), F.col("iso_code").alias("codigo_iso"),
                   F.lit(True).alias("em_co2"), F.lit(False).alias("em_energia"))
    .unionByName(energia_raw.select(F.col("country").alias("nome_localidade"), F.col("iso_code").alias("codigo_iso"),
                                    F.lit(False).alias("em_co2"), F.lit(True).alias("em_energia")))
    .groupBy("nome_localidade")
    .agg(F.max("codigo_iso").alias("codigo_iso"),
         F.max("em_co2").alias("presente_em_co2"),
         F.max("em_energia").alias("presente_em_energia"))
)

# Nome (ou nome alternativo) → código da região. Em caso de conflito, o nome principal tem prioridade sobre o alternativo.
nomes_regioes = (regioes_silver.select("codigo_regiao", F.col("nome_regiao").alias("nome"), F.lit(0).alias("prioridade"))
                 .unionByName(regioes_silver.select("codigo_regiao", F.explode("nomes_alternativos").alias("nome"), F.lit(1).alias("prioridade")))
                 .withColumn("ordem", F.row_number().over(Window.partitionBy("nome").orderBy("prioridade", "codigo_regiao")))
                 .where("ordem = 1")
                 .select(F.col("nome").alias("nome_localidade"), F.col("codigo_regiao").alias("codigo_por_nome")))

atributos_regiao = regioes_silver.select(F.col("codigo_regiao").alias("codigo_localidade"), "tipo_regiao_owid", "is_historico",
                                         "ano_fim_existencia", "continente")

nome = F.col("nome_localidade")
tipo_localidade = (F.when(nome.isin(MUNDO), "Mundo")
                   .when(nome.isin(CONTINENTES), "Continente")
                   .when(nome.isin(GRUPOS_RENDA), "Grupo de renda")
                   .when(nome.isin(BLOCOS_ECONOMICOS), "Bloco econômico")
                   .when(nome.isin(TRANSPORTE_INTERNACIONAL), "Transporte internacional")
                   .when(nome.isin(OUTROS), "Outro")
                   .when(F.coalesce(F.col("is_historico"), F.lit(False)), "País histórico")  # antes do ISO: Antilhas Holandesas têm ISO, mas deixaram de existir em 2010
                   .when(F.col("codigo_iso").isNotNull(), "País")
                   .when(F.col("tipo_regiao_owid") == "country", "País")
                   .otherwise("Agregado de fonte específica"))

localidades = (localidades_brutas
               .join(nomes_regioes, "nome_localidade", "left")
               .withColumn("codigo_localidade", F.coalesce("codigo_iso", "codigo_por_nome"))
               .join(atributos_regiao, "codigo_localidade", "left")
               .withColumn("tipo_localidade", tipo_localidade)
               .withColumn("continente", F.when(F.col("codigo_localidade") == "ATA", "Antártida").otherwise(F.col("continente")))
               .withColumn("is_historico", F.coalesce("is_historico", F.lit(False)))
               .withColumn("descartada", F.col("tipo_localidade") == "Agregado de fonte específica")
               .withColumn("motivo_descarte", F.when(F.col("descartada"),
                           "agregado regional/econômico definido por uma fonte específica; redundante com os agregados da OWID"))
               .select("nome_localidade", "codigo_localidade", "codigo_iso", "tipo_localidade",
                       (F.col("tipo_localidade") == "País").alias("is_pais"), "is_historico", "ano_fim_existencia",
                       "continente", "presente_em_co2", "presente_em_energia", "descartada", "motivo_descarte"))

localidades = salvar_silver(localidades, "localidades")
registrar_etapa("localidades", "classificação das localidades", localidades.count(), localidades.where("NOT descartada").count(),
                "agregados de fontes específicas marcados como descartados")

display(localidades.groupBy("tipo_localidade", "descartada").agg(F.count("*").alias("localidades")).orderBy(F.desc("localidades")))

# COMMAND ----------

display(localidades.where("tipo_localidade NOT IN ('País', 'Agregado de fonte específica')").orderBy("tipo_localidade", "nome_localidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver.emissoes_co2`
# MAGIC
# MAGIC Seleção das colunas usadas nas análises, renomeação (português + unidade), tipagem, junção com as localidades
# MAGIC classificadas e remoção de linhas sem nenhuma medida de emissão.

# COMMAND ----------

# coluna na origem → coluna na silver (a unidade faz parte do nome)
COLUNAS_CO2 = {
    "gdp": "pib_usd_ppc_2011",
    "co2": "co2_mt",
    "coal_co2": "co2_carvao_mt",
    "oil_co2": "co2_petroleo_mt",
    "gas_co2": "co2_gas_mt",
    "cement_co2": "co2_cimento_mt",
    "flaring_co2": "co2_flaring_mt",
    "other_industry_co2": "co2_outras_industrias_mt",
    "land_use_change_co2": "co2_uso_terra_mt",
    "co2_including_luc": "co2_incl_uso_terra_mt",
    "cumulative_co2": "co2_acumulado_mt",
    "cumulative_co2_including_luc": "co2_acumulado_incl_uso_terra_mt",
    "co2_per_capita": "co2_per_capita_t",
    "co2_per_gdp": "co2_por_pib_kg_por_usd",
    "consumption_co2": "co2_consumo_mt",
    "trade_co2": "co2_comercio_liquido_mt",
    "primary_energy_consumption": "energia_primaria_twh",
    "energy_per_capita": "energia_per_capita_kwh",
    "co2_per_unit_energy": "co2_por_energia_g_kwh",
    "share_global_co2": "participacao_co2_mundial_pct",
    "share_global_cumulative_co2": "participacao_co2_acumulado_mundial_pct",
    "total_ghg": "gee_total_mt_co2e",
    "methane": "metano_mt_co2e",
    "nitrous_oxide": "oxido_nitroso_mt_co2e",
}

# "Tem ao menos uma medida?" é calculado sobre as colunas ORIGINAIS: um filtro que referencia dezenas de colunas
# renomeadas ao mesmo tempo faz o otimizador do Spark (propagação de restrições) explodir em memória.
tem_medida_emissao = F.coalesce(*[F.col(origem) for origem in COLUNAS_CO2 if origem != "gdp"]).isNotNull()

emissoes = co2_raw.select(
    F.col("country").alias("nome_localidade"),
    F.expr("try_cast(year AS INT)").alias("ano"),
    num("population").cast("bigint").alias("populacao"),
    *[num(origem).alias(destino) for origem, destino in COLUNAS_CO2.items()],
    tem_medida_emissao.alias("_tem_medida"),
    F.col("_versao_origem"),
)
total_bronze = emissoes.count()

emissoes = emissoes.join(localidades.select("nome_localidade", "codigo_localidade", "tipo_localidade", "descartada"),
                         "nome_localidade", "inner")
emissoes_validas = emissoes.where("NOT descartada").drop("descartada")
registrar_etapa("emissoes_co2", "remoção de agregados de fontes específicas", total_bronze, emissoes_validas.count(),
                "linhas de localidades descartadas (ex.: Africa (GCP), OECD (GCP))")

com_medida = emissoes_validas.where("_tem_medida")
registrar_etapa("emissoes_co2", "remoção de linhas sem nenhuma medida de emissão", emissoes_validas.count(), com_medida.count(),
                "linhas só com população/PIB")

sem_duplicatas = com_medida.dropDuplicates(["nome_localidade", "ano"])
registrar_etapa("emissoes_co2", "deduplicação pela chave (localidade, ano)", com_medida.count(), sem_duplicatas.count(),
                "defensiva: nenhuma duplicata esperada")

emissoes_silver = salvar_silver(
    sem_duplicatas.select("nome_localidade", "codigo_localidade", "tipo_localidade", "ano", "populacao",
                          *COLUNAS_CO2.values(), "_versao_origem"),
    "emissoes_co2")
display(emissoes_silver.where("nome_localidade = 'Brazil' AND ano >= 2020").orderBy("ano"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver.energia`
# MAGIC
# MAGIC Para cada uma das 9 fontes são mantidas quatro medidas: consumo de energia primária (TWh), participação na energia primária (%),
# MAGIC geração de eletricidade (TWh) e participação na eletricidade (%).
# MAGIC
# MAGIC **Bioenergia na eletricidade:** a OWID publica `other_renewable_electricity` (que inclui bioenergia) e, quando existe a abertura,
# MAGIC `biofuel_electricity` e `other_renewable_exc_biofuel_electricity`. Para que "outras renováveis" signifique sempre a mesma coisa,
# MAGIC usamos a versão **sem** bioenergia; quando a abertura não existe, subtraímos a bioenergia (se conhecida) do total.

# COMMAND ----------

# fonte (código OWID) → (nome na silver, coluna de consumo, coluna de participação na energia primária)
FONTES_ENERGIA = {
    "coal": ("carvao", "coal_consumption", "coal_share_energy"),
    "oil": ("petroleo", "oil_consumption", "oil_share_energy"),
    "gas": ("gas_natural", "gas_consumption", "gas_share_energy"),
    "nuclear": ("nuclear", "nuclear_consumption", "nuclear_share_energy"),
    "hydro": ("hidreletrica", "hydro_consumption", "hydro_share_energy"),
    "solar": ("solar", "solar_consumption", "solar_share_energy"),
    "wind": ("eolica", "wind_consumption", "wind_share_energy"),
    "biofuel": ("biocombustiveis", "biofuel_consumption", "biofuel_share_energy"),
    "other_renewable": ("outras_renovaveis", "other_renewable_consumption", "other_renewables_share_energy"),
}


def eletricidade(fonte):
    """Geração elétrica (TWh) e participação na eletricidade (%) de uma fonte, com a regra única para bioenergia."""
    if fonte == "other_renewable":
        twh = F.coalesce(num("other_renewable_exc_biofuel_electricity"),
                         num("other_renewable_electricity") - F.coalesce(num("biofuel_electricity"), F.lit(0.0)))
        pct = F.coalesce(num("other_renewables_share_elec_exc_biofuel"),
                         num("other_renewables_share_elec") - F.coalesce(num("biofuel_share_elec"), F.lit(0.0)))
        return twh, pct
    return num(f"{fonte}_electricity"), num(f"{fonte}_share_elec")


colunas_fontes = []
colunas_origem_energia = []  # colunas originais usadas, para calcular o flag "tem ao menos uma medida"
for fonte, (nome_pt, col_consumo, col_participacao) in FONTES_ENERGIA.items():
    twh_eletricidade, pct_eletricidade = eletricidade(fonte)
    colunas_fontes += [
        num(col_consumo).alias(f"consumo_{nome_pt}_twh"),
        num(col_participacao).alias(f"participacao_{nome_pt}_pct"),
        twh_eletricidade.alias(f"eletricidade_{nome_pt}_twh"),
        pct_eletricidade.alias(f"participacao_eletricidade_{nome_pt}_pct"),
    ]
    colunas_origem_energia += [col_consumo, col_participacao]
    colunas_origem_energia += (["other_renewable_exc_biofuel_electricity", "other_renewable_electricity",
                                "other_renewables_share_elec_exc_biofuel", "other_renewables_share_elec"]
                               if fonte == "other_renewable" else [f"{fonte}_electricity", f"{fonte}_share_elec"])

COLUNAS_TOTAIS_ENERGIA = {
    "primary_energy_consumption": "energia_primaria_twh",
    "low_carbon_share_energy": "participacao_baixo_carbono_pct",
    "renewables_share_energy": "participacao_renovaveis_pct",
    "fossil_share_energy": "participacao_fosseis_pct",
    "electricity_generation": "geracao_eletrica_twh",
    "low_carbon_share_elec": "participacao_eletricidade_baixo_carbono_pct",
    "carbon_intensity_elec": "intensidade_carbono_eletricidade_g_kwh",
}

colunas_origem_energia += list(COLUNAS_TOTAIS_ENERGIA)

energia = energia_raw.select(
    F.col("country").alias("nome_localidade"),
    F.expr("try_cast(year AS INT)").alias("ano"),
    *colunas_fontes,
    *[num(origem).alias(destino) for origem, destino in COLUNAS_TOTAIS_ENERGIA.items()],
    F.coalesce(*[F.col(c) for c in colunas_origem_energia]).isNotNull().alias("_tem_medida"),
    F.col("_versao_origem"),
)
MEDIDAS_ENERGIA = [c for c in energia.columns if c not in ("nome_localidade", "ano", "_versao_origem", "_tem_medida")]
total_bronze = energia.count()

energia = energia.join(localidades.select("nome_localidade", "codigo_localidade", "tipo_localidade", "descartada"),
                       "nome_localidade", "inner")
energia_valida = energia.where("NOT descartada").drop("descartada")
registrar_etapa("energia", "remoção de agregados de fontes específicas", total_bronze, energia_valida.count(),
                "linhas de localidades descartadas (ex.: Europe (EI), OPEC (EIA), G20 (Ember))")

com_medida = energia_valida.where("_tem_medida")
registrar_etapa("energia", "remoção de linhas sem nenhuma medida de energia", energia_valida.count(), com_medida.count(),
                "anos anteriores à cobertura das fontes (só população/PIB)")

# Flag de qualidade: a abertura da eletricidade por fonte fecha com a geração total (± 1%)?
soma_fontes_eletricidade = sum(F.coalesce(F.col(f"eletricidade_{nome_pt}_twh"), F.lit(0.0)) for nome_pt, _, _ in FONTES_ENERGIA.values())
com_flag = com_medida.withColumn(
    "eletricidade_abertura_completa",
    F.when(F.col("geracao_eletrica_twh") > 0,
           F.abs(F.col("geracao_eletrica_twh") - soma_fontes_eletricidade) <= 0.01 * F.col("geracao_eletrica_twh"))
     .otherwise(F.lit(False)))

sem_duplicatas = com_flag.dropDuplicates(["nome_localidade", "ano"])
registrar_etapa("energia", "deduplicação pela chave (localidade, ano)", com_flag.count(), sem_duplicatas.count(),
                "defensiva: nenhuma duplicata esperada")

energia_silver = salvar_silver(
    sem_duplicatas.select("nome_localidade", "codigo_localidade", "tipo_localidade", "ano", *MEDIDAS_ENERGIA,
                          "eletricidade_abertura_completa", "_versao_origem"),
    "energia")

display(energia_silver
        .where("ano >= 2000 AND geracao_eletrica_twh IS NOT NULL")
        .groupBy((F.col("tipo_localidade") == "País").alias("is_pais"))
        .agg(F.count("*").alias("linhas"), F.sum(F.col("eletricidade_abertura_completa").cast("int")).alias("abertura_completa")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Log das transformações

# COMMAND ----------

display(spark.createDataFrame(log_transformacoes,
                              "tabela string, etapa string, linhas_antes long, linhas_depois long, descricao string")
        .withColumn("linhas_removidas", F.col("linhas_antes") - F.col("linhas_depois")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Testes de qualidade da camada Silver
# MAGIC
# MAGIC Se algum teste falhar, o notebook é interrompido (*fail fast*) e a gold não é recarregada com dado ruim.

# COMMAND ----------

def contar(df, condicao):
    return df.where(condicao).count()


silver_emissoes, silver_energia = f"{SCHEMA_SILVER}.emissoes_co2", f"{SCHEMA_SILVER}.energia"
testes = [
    resultado("Unicidade", silver_emissoes, "chaves (localidade, ano) duplicadas",
              emissoes_silver.count() - emissoes_silver.select("nome_localidade", "ano").distinct().count()),
    resultado("Unicidade", silver_energia, "chaves (localidade, ano) duplicadas",
              energia_silver.count() - energia_silver.select("nome_localidade", "ano").distinct().count()),
    resultado("Unicidade", f"{SCHEMA_SILVER}.localidades", "nomes de localidade duplicados",
              localidades.count() - localidades.select("nome_localidade").distinct().count()),
    resultado("Consistência", silver_emissoes, "linhas de agregados de fontes específicas",
              contar(emissoes_silver, "tipo_localidade = 'Agregado de fonte específica'")),
    resultado("Consistência", silver_energia, "linhas de agregados de fontes específicas",
              contar(energia_silver, "tipo_localidade = 'Agregado de fonte específica'")),
    resultado("Completude", f"{SCHEMA_SILVER}.localidades", "países ou países históricos sem código",
              contar(localidades, "tipo_localidade IN ('País', 'País histórico') AND codigo_localidade IS NULL")),
    resultado("Completude", f"{SCHEMA_SILVER}.localidades", "países ou países históricos sem continente",
              contar(localidades, "tipo_localidade IN ('País', 'País histórico') AND continente IS NULL")),
    resultado("Completude", silver_emissoes, "linhas com ano nulo", contar(emissoes_silver, "ano IS NULL")),
    resultado("Completude", silver_energia, "linhas com ano nulo", contar(energia_silver, "ano IS NULL")),
    resultado("Acurácia", silver_emissoes, "co2_mt negativo", contar(emissoes_silver, "co2_mt < 0")),
    resultado("Acurácia", silver_emissoes, "população menor ou igual a zero", contar(emissoes_silver, "populacao <= 0")),
    resultado("Acurácia", silver_energia, "participação fora de [0, 100]",
              contar(energia_silver, " OR ".join(f"{c} < 0 OR {c} > 100" for c in energia_silver.columns if c.startswith("participacao_")))),
]

df_testes = salvar_resultados_qualidade("04 silver", testes, interromper_se_falhar=True)
display(df_testes.select("dimensao", "tabela", "verificacao", "valor_encontrado", "status"))
