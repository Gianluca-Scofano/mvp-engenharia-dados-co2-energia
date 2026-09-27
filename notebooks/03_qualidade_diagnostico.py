# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Diagnóstico de qualidade dos dados (camada Bronze)
# MAGIC
# MAGIC Antes de transformar, é preciso saber **o que está errado** com o dado que chegou. Este notebook examina os atributos das
# MAGIC tabelas bronze nas cinco dimensões pedidas no enunciado (etapa 4.5):
# MAGIC
# MAGIC | Dimensão | Pergunta |
# MAGIC |---|---|
# MAGIC | Completude | Existem valores nulos? Em que proporção? |
# MAGIC | Consistência | Os valores seguem o formato e as regras esperadas (tipos, códigos ISO, somas que deveriam fechar)? |
# MAGIC | Unicidade | Existem duplicatas onde não deveria haver? |
# MAGIC | Acurácia | Os valores fazem sentido no contexto (população positiva, percentuais entre 0 e 100...)? |
# MAGIC | Outliers | Existem valores extremos que podem distorcer as análises? |
# MAGIC
# MAGIC Cada problema encontrado aqui vira uma **decisão de tratamento** implementada na camada silver (notebook 04). Os resultados
# MAGIC das verificações ficam registrados em `governanca.resultados_qualidade`.

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

co2_raw = spark.table(tabela(SCHEMA_BRONZE, "owid_co2_raw"))
energia_raw = spark.table(tabela(SCHEMA_BRONZE, "owid_energia_raw"))

COLUNAS_CONTROLE = {"_arquivo_origem", "_url_origem", "_versao_origem", "_data_ingestao"}
COLUNAS_TEXTO = {"country", "iso_code"}


def colunas_de_dados(df):
    return [c for c in df.columns if c not in COLUNAS_CONTROLE]


def colunas_numericas(df):
    return [c for c in colunas_de_dados(df) if c not in COLUNAS_TEXTO]


def num(coluna):
    """Conversão tolerante: retorna NULL (em vez de erro) quando o texto não é um número."""
    return F.expr(f"try_cast(`{coluna}` AS DOUBLE)")


def contar_por_coluna(df, condicao, colunas):
    """Conta, em uma única passada sobre os dados, quantas linhas satisfazem `condicao(coluna)` em cada coluna."""
    linha = df.select([F.sum(F.when(condicao(c), 1).otherwise(0)).alias(c) for c in colunas]).first()
    return {c: int(linha[c] or 0) for c in colunas}


resultados = []


def registrar(dimensao, tabela_bronze, verificacao, valor, esperado, status, observacao=""):
    resultados.append(resultado(dimensao, tabela_bronze, verificacao, valor, esperado, status, observacao))

# Estilo dos gráficos: superfície clara, grade discreta, uma cor por papel (série / destaque / contexto)
AZUL, LARANJA, VERDE_AGUA, AMARELO, CINZA = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#c3c2b7"
plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
    "axes.edgecolor": "#c3c2b7", "axes.linewidth": 0.8, "axes.labelcolor": "#52514e",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e1e0d9", "grid.linewidth": 0.8, "grid.linestyle": "-", "axes.axisbelow": True,
    "xtick.color": "#898781", "ytick.color": "#898781", "text.color": "#0b0b0b",
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlecolor": "#0b0b0b",
    "font.size": 10, "legend.frameon": False, "lines.linewidth": 2, "lines.solid_capstyle": "round",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Visão geral do que chegou

# COMMAND ----------

def visao_geral(nome, df):
    r = df.select(
        F.count("*").alias("linhas"),
        F.countDistinct("country").alias("localidades"),
        F.min(F.expr("try_cast(year AS INT)")).alias("ano_min"),
        F.max(F.expr("try_cast(year AS INT)")).alias("ano_max"),
    ).first()
    return (nome, r["linhas"], len(colunas_de_dados(df)), r["localidades"], r["ano_min"], r["ano_max"])


display(spark.createDataFrame(
    [visao_geral("bronze.owid_co2_raw", co2_raw), visao_geral("bronze.owid_energia_raw", energia_raw)],
    "tabela string, linhas long, colunas long, localidades long, ano_min int, ano_max int",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Completude: percentual de nulos por atributo
# MAGIC
# MAGIC Calculado para **todas** as colunas das duas tabelas (79 + 130 atributos). A tabela abaixo fica ordenada da coluna menos
# MAGIC preenchida para a mais preenchida.

# COMMAND ----------

def completude(nome, df):
    colunas = colunas_de_dados(df)
    nulos = contar_por_coluna(df, lambda c: F.col(c).isNull(), colunas)
    total = df.count()
    return pd.DataFrame({
        "tabela": nome,
        "coluna": colunas,
        "nulos": [nulos[c] for c in colunas],
        "pct_nulos": [round(100 * nulos[c] / total, 1) for c in colunas],
    })


completude_pdf = pd.concat([completude("owid_co2_raw", co2_raw), completude("owid_energia_raw", energia_raw)])
display(spark.createDataFrame(completude_pdf.sort_values("pct_nulos", ascending=False)))

# COMMAND ----------

ATRIBUTOS_CHAVE = {
    "owid_co2_raw": ["country", "year", "iso_code", "population", "gdp", "co2", "co2_per_capita", "coal_co2", "oil_co2",
                     "gas_co2", "land_use_change_co2", "co2_per_unit_energy", "consumption_co2"],
    "owid_energia_raw": ["primary_energy_consumption", "coal_share_energy", "low_carbon_share_energy",
                         "electricity_generation", "low_carbon_share_elec"],
}
chave = pd.concat([completude_pdf[(completude_pdf.tabela == t) & (completude_pdf.coluna.isin(cols))]
                   for t, cols in ATRIBUTOS_CHAVE.items()])
chave = chave.assign(rotulo=chave.coluna + "  (" + chave.tabela.str.replace("owid_", "").str.replace("_raw", "") + ")",
                     pct_preenchido=100 - chave.pct_nulos).sort_values("pct_preenchido")

fig, ax = plt.subplots(figsize=(9, 6))
ax.barh(chave.rotulo, chave.pct_preenchido, color=AZUL, height=0.6)
for y, v in enumerate(chave.pct_preenchido):
    ax.text(v + 1, y, f"{v:.0f}%", va="center", fontsize=9, color="#52514e")
ax.set_xlim(0, 110)
ax.set_xlabel("% das linhas com valor preenchido")
ax.set_title("Completude dos atributos-chave na camada bronze")
ax.grid(axis="y", visible=False)
plt.tight_layout()
plt.show()

for tabela_bronze, cols in ATRIBUTOS_CHAVE.items():
    for c in cols:
        pct = float(completude_pdf[(completude_pdf.tabela == tabela_bronze) & (completude_pdf.coluna == c)].pct_nulos.iloc[0])
        registrar("Completude", tabela_bronze, f"% de nulos em {c}", pct, "informativo",
                  "ALERTA" if pct > 50 else "OK", "cobertura depende do ano e da localidade (ver seção 8)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consistência
# MAGIC
# MAGIC ### 3.1 Tipos: todo valor numérico é de fato um número?
# MAGIC Como a bronze guarda tudo como texto, verificamos se existe algum valor preenchido que **não** pode ser convertido para número.

# COMMAND ----------

tipos = []
for nome, df in [("owid_co2_raw", co2_raw), ("owid_energia_raw", energia_raw)]:
    cols = colunas_numericas(df)
    invalidos = contar_por_coluna(df, lambda c: F.col(c).isNotNull() & num(c).isNull(), cols)
    total_invalidos = sum(invalidos.values())
    tipos.append((nome, len(cols), total_invalidos, [c for c, n in invalidos.items() if n > 0]))
    registrar("Consistência", nome, "valores não numéricos em colunas numéricas", total_invalidos, "0",
              "OK" if total_invalidos == 0 else "FALHA")

display(spark.createDataFrame(tipos, "tabela string, colunas_numericas long, valores_nao_numericos long, colunas_com_problema array<string>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Códigos de país (ISO 3166-1 alfa-3)
# MAGIC Todo código preenchido deve ter 3 letras maiúsculas, e um mesmo código deve corresponder ao mesmo nome nas duas bases.

# COMMAND ----------

iso = []
for nome, df in [("owid_co2_raw", co2_raw), ("owid_energia_raw", energia_raw)]:
    r = df.select(
        F.countDistinct("country").alias("localidades"),
        F.countDistinct(F.when(F.col("iso_code").isNull(), F.col("country"))).alias("localidades_sem_iso"),
        F.sum(F.when(F.col("iso_code").isNotNull() & ~F.col("iso_code").rlike("^[A-Z]{3}$"), 1).otherwise(0)).alias("iso_fora_do_padrao"),
    ).first()
    iso.append((nome, r["localidades"], r["localidades_sem_iso"], int(r["iso_fora_do_padrao"] or 0)))
    registrar("Consistência", nome, "códigos ISO fora do padrão ^[A-Z]{3}$", r["iso_fora_do_padrao"] or 0, "0",
              "OK" if not r["iso_fora_do_padrao"] else "FALHA")
    registrar("Consistência", nome, "localidades sem código ISO", r["localidades_sem_iso"], "informativo", "ALERTA",
              "agregados (Mundo, continentes, grupos de renda...) e países históricos, ver seção 6")

pares_co2 = co2_raw.select("iso_code", F.col("country").alias("nome_co2")).where("iso_code IS NOT NULL").distinct()
pares_energia = energia_raw.select("iso_code", F.col("country").alias("nome_energia")).where("iso_code IS NOT NULL").distinct()
divergentes = pares_co2.join(pares_energia, "iso_code").where("nome_co2 <> nome_energia").count()
registrar("Consistência", "co2 x energia", "mesmo código ISO com nomes diferentes entre as bases", divergentes, "0",
          "OK" if divergentes == 0 else "FALHA")

display(spark.createDataFrame(iso, "tabela string, localidades long, localidades_sem_iso long, iso_fora_do_padrao long"))
print(f"Códigos ISO com nomes diferentes entre as duas bases: {divergentes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Somas que deveriam fechar
# MAGIC
# MAGIC - **CO₂:** o total (`co2`) deve ser a soma das emissões por origem (carvão, petróleo, gás, cimento, queima em flare e outras indústrias).
# MAGIC - **Energia primária:** o total (`primary_energy_consumption`) deveria ser a soma das 9 fontes.
# MAGIC - **Eletricidade:** a geração total (`electricity_generation`) deve ser a soma da geração por fonte.

# COMMAND ----------

FONTES_CO2 = ["coal_co2", "oil_co2", "gas_co2", "cement_co2", "flaring_co2", "other_industry_co2"]
soma_co2 = sum(F.coalesce(num(c), F.lit(0.0)) for c in FONTES_CO2)
alguma_origem = F.coalesce(*[num(c) for c in FONTES_CO2]).isNotNull()
checagem_co2 = (co2_raw
                .where(num("co2") > 0)
                .select(alguma_origem.alias("tem_origem"),
                        F.abs(num("co2") - soma_co2).alias("desvio_mt"),
                        (F.abs(num("co2") - soma_co2) / num("co2") * 100).alias("desvio_pct"))
                .agg(F.sum(F.when(~F.col("tem_origem"), 1).otherwise(0)).alias("so_total"),
                     F.sum(F.when(F.col("tem_origem"), 1).otherwise(0)).alias("linhas"),
                     # desvio relevante: acima de 1% E acima de 0,01 Mt (abaixo disso é arredondamento de valores minúsculos)
                     F.sum(F.when(F.col("tem_origem") & (F.col("desvio_pct") > 1) & (F.col("desvio_mt") > 0.01), 1).otherwise(0)).alias("desvios"))
                .first())

FONTES_PRIMARIA = ["coal", "oil", "gas", "nuclear", "hydro", "solar", "wind", "biofuel", "other_renewable"]
soma_primaria = sum(num(f"{f}_consumption") for f in FONTES_PRIMARIA)  # nulo se qualquer fonte for nula
gap_primaria = (energia_raw
                .where(num("primary_energy_consumption") > 0)
                .select(F.col("country"), F.col("year"),
                        ((num("primary_energy_consumption") - soma_primaria) / num("primary_energy_consumption") * 100).alias("gap_pct"))
                .where("gap_pct IS NOT NULL"))
resumo_gap = gap_primaria.agg(F.count("*").alias("linhas"),
                              F.round(F.expr("percentile_approx(gap_pct, 0.5)"), 2).alias("mediana_gap_pct"),
                              F.round(F.expr("percentile_approx(gap_pct, 0.95)"), 2).alias("p95_gap_pct")).first()
gap_mundo = gap_primaria.where("country = 'World' AND year = '2024'").first()

# a soma das participações das fontes de baixo carbono reproduz a participação de baixo carbono publicada?
FONTES_BAIXO_CARBONO = ["nuclear", "hydro", "solar", "wind", "biofuel", "other_renewables"]
soma_participacoes = sum(num(f"{f}_share_energy") for f in FONTES_BAIXO_CARBONO)
checagem_participacao = (energia_raw
                         .where(num("low_carbon_share_energy").isNotNull() & soma_participacoes.isNotNull())
                         .select(F.abs(soma_participacoes - num("low_carbon_share_energy")).alias("diferenca_pp"))
                         .agg(F.count("*").alias("linhas"), F.max("diferenca_pp").alias("maior_diferenca_pp"),
                              F.sum(F.when(F.col("diferenca_pp") > 0.1, 1).otherwise(0)).alias("acima_0_1pp"))
                         .first())

FONTES_ELETRICIDADE = ["coal", "oil", "gas", "nuclear", "hydro", "solar", "wind"]
soma_eletricidade = sum(F.coalesce(num(f"{f}_electricity"), F.lit(0.0)) for f in FONTES_ELETRICIDADE) \
    + F.coalesce(num("other_renewable_electricity"), F.lit(0.0))
checagem_eletricidade = (energia_raw
                         .where(num("electricity_generation") > 0)
                         .select(F.when(F.col("iso_code").isNotNull(), "país").otherwise("agregado").alias("tipo"),
                                 F.when(F.expr("try_cast(year AS INT)") >= 2000, "2000 em diante").otherwise("antes de 2000").alias("periodo"),
                                 (F.abs(num("electricity_generation") - soma_eletricidade) / num("electricity_generation") * 100).alias("desvio_pct"))
                         .groupBy("tipo", "periodo")
                         .agg(F.count("*").alias("linhas"), F.sum(F.when(F.col("desvio_pct") > 1, 1).otherwise(0)).alias("desvios"))
                         .collect())
eletricidade = {(r["tipo"], r["periodo"]): r for r in checagem_eletricidade}
desvio_paises_recente = eletricidade.get(("país", "2000 em diante"))["desvios"]
desvio_paises_antigo = eletricidade.get(("país", "antes de 2000"))["desvios"]

display(spark.createDataFrame([
    ("CO2 total = soma das origens (linhas com alguma origem informada)", checagem_co2["linhas"], int(checagem_co2["desvios"]),
     f"{checagem_co2['so_total']} linhas trazem só o total, sem abertura por origem (agregados de fontes)"),
    ("Energia primária total = soma das 9 fontes", resumo_gap["linhas"], None,
     f"a soma fica abaixo do total: gap mediano {resumo_gap['mediana_gap_pct']}%, p95 {resumo_gap['p95_gap_pct']}%, Mundo 2024 {gap_mundo['gap_pct']:.2f}%"),
    ("Participação de baixo carbono = soma das participações das fontes", checagem_participacao["linhas"], int(checagem_participacao["acima_0_1pp"]),
     f"maior diferença: {checagem_participacao['maior_diferenca_pp']:.3f} p.p."),
    ("Geração elétrica = soma das fontes (países, 2000 em diante)", eletricidade[("país", "2000 em diante")]["linhas"], int(desvio_paises_recente), ""),
    ("Geração elétrica = soma das fontes (países, antes de 2000)", eletricidade[("país", "antes de 2000")]["linhas"], int(desvio_paises_antigo),
     "abertura por fonte incompleta nos anos antigos"),
], "regra string, linhas_verificadas long, linhas_com_desvio long, observacao string"))

registrar("Consistência", "owid_co2_raw", "linhas em que co2 difere da soma das origens (> 1% e > 0,01 Mt)",
          checagem_co2["desvios"], "0", "OK" if checagem_co2["desvios"] == 0 else "ALERTA")
registrar("Consistência", "owid_energia_raw", "gap mediano (%) entre energia primária total e soma das fontes",
          resumo_gap["mediana_gap_pct"], "≈ 0", "ALERTA",
          "a soma das fontes não fecha o total: participações devem vir das colunas *_share_energy, calculadas sobre o total")
registrar("Consistência", "owid_energia_raw", "linhas em que a soma das participações de baixo carbono difere da publicada (> 0,1 p.p.)",
          checagem_participacao["acima_0_1pp"], "0", "OK" if checagem_participacao["acima_0_1pp"] == 0 else "ALERTA")
registrar("Consistência", "owid_energia_raw", "países com geração elétrica ≠ soma das fontes (> 1%), 2000 em diante",
          desvio_paises_recente, "0", "OK" if desvio_paises_recente == 0 else "ALERTA")
registrar("Consistência", "owid_energia_raw", "países com geração elétrica ≠ soma das fontes (> 1%), antes de 2000",
          desvio_paises_antigo, "0", "ALERTA" if desvio_paises_antigo else "OK",
          "a matriz elétrica por fonte só é usada a partir de 2000")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Unicidade
# MAGIC A granularidade esperada das duas bases é **uma linha por localidade e ano**.

# COMMAND ----------

unicidade = []
for nome, df in [("owid_co2_raw", co2_raw), ("owid_energia_raw", energia_raw)]:
    dados = df.select(colunas_de_dados(df))
    total = dados.count()
    duplicadas_chave = total - dados.select("country", "year").distinct().count()
    duplicadas_completas = total - dados.distinct().count()
    unicidade.append((nome, total, duplicadas_chave, duplicadas_completas))
    registrar("Unicidade", nome, "linhas duplicadas na chave (country, year)", duplicadas_chave, "0",
              "OK" if duplicadas_chave == 0 else "FALHA")
    registrar("Unicidade", nome, "linhas completamente duplicadas", duplicadas_completas, "0",
              "OK" if duplicadas_completas == 0 else "FALHA")

regioes_raw = spark.table(tabela(SCHEMA_BRONZE, "owid_regioes_raw"))
codigos_duplicados = regioes_raw.count() - regioes_raw.select("code").distinct().count()
registrar("Unicidade", "owid_regioes_raw", "códigos de região duplicados", codigos_duplicados, "0",
          "OK" if codigos_duplicados == 0 else "FALHA")
unicidade.append(("owid_regioes_raw (chave: code)", regioes_raw.count(), codigos_duplicados, None))

display(spark.createDataFrame(unicidade, "tabela string, linhas long, duplicadas_na_chave long, duplicadas_completas long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia: os valores respeitam o domínio esperado?
# MAGIC
# MAGIC Regras de domínio verificadas em cada atributo usado no modelo. Valores negativos só são aceitáveis onde têm significado físico:
# MAGIC emissões por **mudança no uso da terra** podem ser negativas (reflorestamento transforma o país em sumidouro de carbono) e
# MAGIC emissões **embutidas no comércio** são negativas para exportadores líquidos.

# COMMAND ----------

REGRAS_CO2 = [
    ("year", "entre 1750 e 2025", lambda c: (num(c) < 1750) | (num(c) > 2025) | (num(c) != F.floor(num(c)))),
    ("population", "> 0", lambda c: num(c) <= 0),
    ("gdp", "> 0", lambda c: num(c) <= 0),
] + [(c, ">= 0", lambda c: num(c) < 0) for c in
     ["co2", "coal_co2", "oil_co2", "gas_co2", "cement_co2", "flaring_co2", "other_industry_co2", "cumulative_co2",
      "co2_per_capita", "consumption_co2", "primary_energy_consumption", "co2_per_unit_energy", "methane", "nitrous_oxide"]] + [
    ("share_global_co2", "entre 0 e 100", lambda c: (num(c) < 0) | (num(c) > 100)),
    ("share_global_cumulative_co2", "entre 0 e 100", lambda c: (num(c) < 0) | (num(c) > 100)),
]
COLUNAS_PARTICIPACAO = [c for c in energia_raw.columns if "share" in c and not c.startswith("net_elec")]
REGRAS_ENERGIA = [
    ("year", "entre 1750 e 2025", lambda c: (num(c) < 1750) | (num(c) > 2025) | (num(c) != F.floor(num(c)))),
] + [(f"{f}_consumption", ">= 0", lambda c: num(c) < 0) for f in FONTES_PRIMARIA] \
  + [(f"{f}_electricity", ">= 0", lambda c: num(c) < 0) for f in FONTES_ELETRICIDADE + ["other_renewable", "biofuel", "other_renewable_exc_biofuel"]] \
  + [(c, "entre 0 e 100", lambda c: (num(c) < 0) | (num(c) > 100)) for c in COLUNAS_PARTICIPACAO]
NEGATIVOS_VALIDOS = ["land_use_change_co2", "co2_including_luc", "trade_co2"]

acuracia = []
for nome, df, regras in [("owid_co2_raw", co2_raw, REGRAS_CO2), ("owid_energia_raw", energia_raw, REGRAS_ENERGIA)]:
    linha = df.select([F.sum(F.when(regra(col), 1).otherwise(0)).alias(f"r{i}") for i, (col, _, regra) in enumerate(regras)]).first()
    for i, (col, dominio, _) in enumerate(regras):
        violacoes = int(linha[f"r{i}"] or 0)
        acuracia.append((nome, col, dominio, violacoes))
        registrar("Acurácia", nome, f"{col}: {dominio}", violacoes, "0", "OK" if violacoes == 0 else "FALHA")

negativos = co2_raw.select([F.sum(F.when(num(c) < 0, 1).otherwise(0)).alias(c) for c in NEGATIVOS_VALIDOS]).first()
for c in NEGATIVOS_VALIDOS:
    acuracia.append(("owid_co2_raw", c, "negativo permitido (informativo)", int(negativos[c] or 0)))
    registrar("Acurácia", "owid_co2_raw", f"{c}: valores negativos (válidos)", negativos[c] or 0, "informativo", "OK",
              "sumidouro de carbono / exportador líquido de emissões")

acuracia_df = spark.createDataFrame(acuracia, "tabela string, coluna string, dominio_esperado string, violacoes long")
regras_violadas = acuracia_df.where("violacoes > 0 AND dominio_esperado NOT LIKE 'negativo permitido%'").count()
print(f"Regras verificadas: {acuracia_df.count()} | regras com violação: {regras_violadas}")
display(acuracia_df.orderBy(F.desc("violacoes"), "tabela", "coluna"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Localidades: países misturados com agregados (o problema mais sério)
# MAGIC
# MAGIC A coluna `country` não contém só países. Ela mistura, na mesma tabela e no mesmo nível, o **Mundo**, **continentes**,
# MAGIC **grupos de renda**, **blocos econômicos**, **países que não existem mais** (URSS, Iugoslávia...) e dezenas de
# MAGIC **agregados definidos por fontes específicas** (ex.: `Europe (EI)`, `Africa (EIA)`, `OECD (Ember)`).
# MAGIC Somar a coluna sem tratar isso conta a mesma emissão várias vezes.

# COMMAND ----------

soma_ingenua = co2_raw.where("year = '2024'").agg(F.sum(num("co2")).alias("v")).first()["v"]
soma_paises = co2_raw.where("year = '2024' AND iso_code IS NOT NULL").agg(F.sum(num("co2")).alias("v")).first()["v"]
transporte_internacional = (co2_raw.where("year = '2024' AND country IN ('International aviation', 'International shipping')")
                            .agg(F.sum(num("co2")).alias("v")).first()["v"])
mundo = co2_raw.where("year = '2024' AND country = 'World'").select(num("co2").alias("v")).first()["v"]

display(spark.createDataFrame([
    ("Soma ingênua de todas as linhas", float(soma_ingenua), float(soma_ingenua / mundo)),
    ("Soma apenas das localidades com código ISO (países)", float(soma_paises), float(soma_paises / mundo)),
    ("Países + transporte internacional (aviação e navegação)", float(soma_paises + transporte_internacional),
     float((soma_paises + transporte_internacional) / mundo)),
    ("Valor publicado para 'World'", float(mundo), 1.0),
], "calculo string, co2_2024_mt double, razao_sobre_mundo double"))

registrar("Consistência", "owid_co2_raw", "soma ingênua das linhas de 2024 / total do Mundo", soma_ingenua / mundo, "1,0", "FALHA",
          "agregados misturados com países: exige classificar as localidades na silver")
registrar("Consistência", "owid_co2_raw", "(países + transporte internacional) / Mundo em 2024",
          (soma_paises + transporte_internacional) / mundo, "≈ 1,0",
          "OK" if abs((soma_paises + transporte_internacional) / mundo - 1) < 0.005 else "ALERTA",
          "confirma que, separados os agregados, as partes somam o todo")

# COMMAND ----------

sem_iso = (co2_raw.select("country", F.lit("co2").alias("base")).where("iso_code IS NULL")
           .unionByName(energia_raw.select("country", F.lit("energia").alias("base")).where("iso_code IS NULL"))
           .groupBy("country").agg(F.concat_ws(" + ", F.sort_array(F.collect_set("base"))).alias("presente_em"))
           .orderBy("country"))
print(f"Localidades sem código ISO: {sem_iso.count()}")
display(sem_iso)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Outliers: emissões per capita
# MAGIC
# MAGIC Método do intervalo interquartil (IQR) sobre o CO₂ per capita dos países em 2024: é outlier o valor acima de Q3 + 1,5 × IQR.

# COMMAND ----------

per_capita = (co2_raw
              .where(f"year = '{ANO_REFERENCIA}' AND iso_code IS NOT NULL")
              .select("country", num("population").alias("populacao"), num("co2_per_capita").alias("co2_per_capita"))
              .where("co2_per_capita IS NOT NULL")
              .toPandas())
q1, q3 = per_capita.co2_per_capita.quantile([0.25, 0.75])
limite = q3 + 1.5 * (q3 - q1)
outliers = per_capita[per_capita.co2_per_capita > limite].sort_values("co2_per_capita", ascending=False)
pequenos = int((outliers.populacao < POPULACAO_MINIMA_RANKING_PER_CAPITA).sum())

print(f"Q1 = {q1:.2f} t | Q3 = {q3:.2f} t | limite superior = {limite:.2f} t por pessoa")
print(f"{len(outliers)} países acima do limite, {pequenos} deles com menos de 1 milhão de habitantes")
display(spark.createDataFrame(outliers.assign(populacao=outliers.populacao.astype("int64"))))

registrar("Outliers", "owid_co2_raw", f"países com CO2 per capita acima de Q3 + 1,5×IQR em {ANO_REFERENCIA}", len(outliers),
          "informativo", "ALERTA", "valores reais (petroestados e microterritórios): mantidos, mas rankings per capita usam população mínima")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(9, 3.2))
ax.boxplot(per_capita.co2_per_capita, vert=False, widths=0.5, patch_artist=True,
           boxprops={"facecolor": "#cde2fb", "edgecolor": AZUL}, medianprops={"color": AZUL, "linewidth": 2},
           whiskerprops={"color": "#898781"}, capprops={"color": "#898781"},
           flierprops={"marker": "o", "markerfacecolor": LARANJA, "markeredgecolor": "#fcfcfb", "markersize": 7})
ROTULOS = {"Qatar": "Catar", "Kuwait": "Kuwait e Brunei", "Saudi Arabia": "Arábia Saudita e EAU", "United States": "EUA e Austrália"}
for pais, rotulo in ROTULOS.items():
    valor = per_capita[per_capita.country == pais].co2_per_capita.iloc[0]
    ax.annotate(rotulo, (valor, 1), xytext=(0, 14), textcoords="offset points", ha="center", fontsize=8, color="#52514e")
brasil = per_capita[per_capita.country == "Brazil"].co2_per_capita.iloc[0]
ax.axvline(brasil, color="#52514e", linewidth=1)
ax.annotate(f"Brasil: {brasil:.1f} t", (brasil, 0.62), xytext=(4, 0), textcoords="offset points", fontsize=8, color="#52514e")
ax.set_yticks([])
ax.set_xlabel(f"t de CO₂ por pessoa ({ANO_REFERENCIA})")
ax.set_title(f"Distribuição do CO₂ per capita entre países em {ANO_REFERENCIA}: outliers em laranja")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cobertura temporal: quantos países têm dado em cada ano?
# MAGIC
# MAGIC Nem toda métrica existe para todos os anos. Esse levantamento define os **anos de referência** usados nas análises.

# COMMAND ----------

cobertura = (co2_raw.where("iso_code IS NOT NULL")
             .groupBy(F.expr("try_cast(year AS INT)").alias("ano"))
             .agg(F.count(num("co2")).alias("CO₂ (emissões)"), F.count(num("gdp")).alias("PIB"))
             .join(energia_raw.where("iso_code IS NOT NULL")
                   .groupBy(F.expr("try_cast(year AS INT)").alias("ano"))
                   .agg(F.count(num("low_carbon_share_energy")).alias("Matriz de energia primária por fonte"),
                        F.count(num("low_carbon_share_elec")).alias("Matriz elétrica por fonte")),
                   "ano", "outer")
             .where("ano >= 1950")
             .orderBy("ano")
             .toPandas()
             .fillna(0))

fig, ax = plt.subplots(figsize=(10, 4.6))
for serie, cor in zip(["CO₂ (emissões)", "PIB", "Matriz elétrica por fonte", "Matriz de energia primária por fonte"],
                      [AZUL, LARANJA, VERDE_AGUA, AMARELO]):
    com_dado = cobertura[cobertura[serie] > 0]  # anos sem nenhum país ficam fora da linha (sem "queda" artificial para zero)
    ax.plot(com_dado.ano, com_dado[serie], color=cor, label=serie)
    ultimo = com_dado.iloc[-1]
    ax.plot(ultimo.ano, ultimo[serie], "o", color=cor, markersize=6, markeredgecolor="#fcfcfb", markeredgewidth=1.5)
    ax.annotate(f"{int(ultimo[serie])} em {int(ultimo.ano)}", (ultimo.ano, ultimo[serie]), xytext=(6, 0),
                textcoords="offset points", va="center", fontsize=8, color="#52514e")
ax.set_xlim(1950, 2033)
ax.set_ylim(0, 240)
ax.set_ylabel("países com dado")
ax.set_title("Cobertura das métricas por ano (países com código ISO)")
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=4)
plt.tight_layout()
plt.show()

display(spark.createDataFrame(cobertura[cobertura.ano >= 2018].astype("int64")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Registro dos resultados em `governanca.resultados_qualidade`

# COMMAND ----------

df_resultados = salvar_resultados_qualidade("03 diagnóstico (bronze)", resultados)

display(df_resultados.groupBy("dimensao", "status").count().orderBy("dimensao", "status"))

# COMMAND ----------

display(df_resultados.where("status <> 'OK'").select("dimensao", "tabela", "verificacao", "valor_encontrado", "status", "observacao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumo: problemas encontrados → tratamento aplicado
# MAGIC
# MAGIC | # | Dimensão | Problema encontrado (evidência) | Tratamento | Onde |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Consistência | **Países misturados com agregados** (Mundo, continentes, grupos de renda, blocos): a soma ingênua das linhas de 2024 dá **6,36× o total mundial** | Cada localidade recebe um `tipo_localidade` e a flag `is_pais`; somas e rankings usam só países | 04 silver / 05 gold |
# MAGIC | 2 | Consistência | **94 agregados definidos por fontes específicas** (`Europe (EI)`, `Africa (GCP)`, `OECD (Ember)`, `Europe (excl. EU-27)`...), redundantes e sobrepostos | Descartados da silver (3.559 linhas de CO₂ e 4.270 de energia); continuam na bronze | 04 silver |
# MAGIC | 3 | Completude | **Localidades sem código ISO** (36 no CO₂, 94 na energia), incluindo Kosovo e países históricos (URSS, Iugoslávia...) | Código obtido do `regions.yml` por nome/nome alternativo (ex.: `OWID_KOS`, `OWID_USS`) | 04 silver |
# MAGIC | 4 | Completude | **Nenhuma informação de continente** nas bases | Continente derivado da composição oficial dos continentes no `regions.yml` (validado: soma por continente = agregado da OWID) | 04 silver / 07 |
# MAGIC | 5 | Completude | **Cobertura desigual por métrica e ano**: PIB só até 2022; matriz por fonte só para 79 países; emissões de consumo para ~120 países; 2025 parcial | Anos de referência definidos: 2024 (emissões e energia) e 2022 (PIB); limitações documentadas | 00 config / 07 |
# MAGIC | 6 | Completude | **Linhas sem nenhuma medida** (só população/PIB) | Removidas das tabelas silver (1.585 de CO₂ e 7.375 de energia) | 04 silver |
# MAGIC | 7 | Consistência | **Energia primária total ≠ soma das 9 fontes** (gap mediano 0,44%; 0,83% no Mundo em 2024) | Participações por fonte vêm das colunas `*_share_energy`, calculadas pela OWID sobre o total; teste confirma que a soma das participações de baixo carbono reproduz o valor publicado (diferença máx. 0,002 p.p.) | 04 silver / 05 gold |
# MAGIC | 8 | Consistência | **Geração elétrica ≠ soma das fontes antes de 2000** (545 de 1.190 linhas de países; 0 a partir de 2000) | Flag `eletricidade_abertura_completa`; na gold, geração por fonte só é carregada quando a abertura fecha com o total | 04 silver / 05 gold |
# MAGIC | 9 | Consistência | **Bioenergia** na eletricidade às vezes vem separada, às vezes embutida em "outras renováveis" | Regra única: "outras renováveis" sempre **sem** bioenergia | 04 silver |
# MAGIC | 10 | Outliers | **13 países acima de Q3 + 1,5×IQR** no CO₂ per capita em 2024 (petroestados e 3 microterritórios); na série histórica, Sint Maarten nos anos 1950 (até 783 t/pessoa, < 3 mil habitantes) e Kuwait em 1991 (365 t/pessoa, incêndios de poços na Guerra do Golfo) | Valores reais ou explicáveis: mantidos; rankings per capita só com países de ≥ 1 milhão de habitantes | 07 análise |
# MAGIC | 11 | Consistência | **Tudo chega como texto** (0 valores não numéricos em 205 colunas numéricas) | Conversão explícita com `try_cast` e renomeação com unidade no nome | 04 silver |
# MAGIC | 12 | Unicidade | 0 duplicatas na chave (localidade, ano) e 0 linhas duplicadas | Deduplicação defensiva + teste de unicidade a cada carga | 04 silver / 05 gold |
# MAGIC | 13 | Acurácia | 68 regras de domínio verificadas, 0 violações; negativos só onde são válidos (uso da terra: 5.708; comércio: 1.597) | Regras viram **CHECK constraints** na gold, que bloqueiam cargas futuras fora do domínio | 05 gold |
