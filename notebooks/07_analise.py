# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Análise: respondendo às perguntas de negócio
# MAGIC
# MAGIC Todas as consultas usam **apenas a camada gold** (modelo dimensional). Regra adotada em todas elas: rankings e somas entre
# MAGIC países filtram `dim_localidade.is_pais = true`, evitando a dupla contagem identificada no diagnóstico de qualidade.
# MAGIC
# MAGIC **Problema:** entender quem é responsável pelas emissões de CO₂, se crescer economicamente ainda exige emitir mais e se a
# MAGIC transição para fontes de baixo carbono está de fato reduzindo o carbono da energia, com atenção especial à posição do Brasil.
# MAGIC
# MAGIC | # | Pergunta |
# MAGIC |---|---|
# MAGIC | P1 | Quais países mais emitem CO₂ hoje e quais mais emitiram ao longo da história? Quão concentradas são as emissões? |
# MAGIC | P2 | O ranking muda quando consideramos o tamanho da população? Onde o Brasil se posiciona? |
# MAGIC | P3 | É possível crescer sem emitir mais? Quais países aumentaram o PIB e reduziram as emissões entre 2000 e 2022? |
# MAGIC | P4 | A transição energética está acontecendo? Como evoluiu a participação das fontes de baixo carbono na energia do mundo e do Brasil? |
# MAGIC | P5 | Países com mais energia de baixo carbono têm, de fato, energia menos intensiva em carbono? |
# MAGIC | P6 | Qual é o peso do desmatamento (mudança no uso da terra) nas emissões do Brasil, comparado a outros grandes emissores? |
# MAGIC | P7 | Como a distribuição geográfica das emissões mudou entre 1950, 1990 e 2024? |

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

DIM_LOCALIDADE = tabela(SCHEMA_GOLD, "dim_localidade")
DIM_ANO = tabela(SCHEMA_GOLD, "dim_ano")
DIM_FONTE = tabela(SCHEMA_GOLD, "dim_fonte_energia")
FATO_EMISSOES = tabela(SCHEMA_GOLD, "fato_emissoes_anual")
FATO_ENERGIA = tabela(SCHEMA_GOLD, "fato_energia_fonte")


def consultar(sql, visao=None):
    """Executa a consulta, exibe o resultado e devolve um DataFrame pandas (resultados pequenos) para o gráfico.
    Se `visao` for informada, o resultado também fica disponível como view temporária para as consultas seguintes."""
    df = spark.sql(sql)
    if visao:
        df.createOrReplaceTempView(visao)
    display(df)
    return df.toPandas()


# Estilo: superfície clara, grade discreta; azul = série, laranja = destaque (Brasil), cinza = contexto
AZUL, LARANJA, VERDE_AGUA, AMARELO, MAGENTA, VERDE = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"
CINZA, TINTA_SECUNDARIA = "#c3c2b7", "#52514e"
plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
    "axes.edgecolor": "#c3c2b7", "axes.linewidth": 0.8, "axes.labelcolor": TINTA_SECUNDARIA,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e1e0d9", "grid.linewidth": 0.8, "grid.linestyle": "-", "axes.axisbelow": True,
    "xtick.color": "#898781", "ytick.color": "#898781", "text.color": "#0b0b0b",
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlecolor": "#0b0b0b",
    "font.size": 10, "legend.frameon": False, "lines.linewidth": 2, "lines.solid_capstyle": "round",
})


def barras_horizontais(ax, rotulos, valores, cores, formato="{:,.1f}", titulo="", eixo_x=""):
    posicoes = range(len(rotulos))
    ax.barh(list(posicoes), valores, color=cores, height=0.62)
    ax.set_yticks(list(posicoes))
    ax.set_yticklabels(rotulos)
    ax.invert_yaxis()
    limite = max(valores) * 1.18
    for y, v in zip(posicoes, valores):
        ax.text(v + limite * 0.01, y, formato.format(v), va="center", fontsize=8.5, color=TINTA_SECUNDARIA)
    ax.set_xlim(0, limite)
    ax.grid(axis="y", visible=False)
    ax.set_title(titulo)
    ax.set_xlabel(eixo_x)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Quem mais emite hoje e quem mais emitiu na história?
# MAGIC
# MAGIC Emissões fósseis e industriais de CO₂ (sem uso da terra) em 2024 e acumuladas desde 1750. A participação no total mundial
# MAGIC (`participacao_co2_mundial_pct`) é a publicada pela OWID, cujo denominador inclui o transporte internacional.

# COMMAND ----------

p1 = consultar(f"""
WITH paises AS (
    SELECT l.nome_localidade AS pais,
           f.co2_mt,
           f.participacao_co2_mundial_pct,
           f.co2_acumulado_mt,
           f.participacao_co2_acumulado_mundial_pct,
           RANK() OVER (ORDER BY f.co2_mt DESC) AS ranking_2024,
           RANK() OVER (ORDER BY f.co2_acumulado_mt DESC) AS ranking_historico
    FROM {FATO_EMISSOES} f
    JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.is_pais AND f.ano = {ANO_REFERENCIA} AND f.co2_mt IS NOT NULL
)
SELECT pais, ranking_2024, ROUND(co2_mt, 0) AS co2_2024_mt, ROUND(participacao_co2_mundial_pct, 1) AS participacao_2024_pct,
       ranking_historico, ROUND(co2_acumulado_mt / 1000, 1) AS co2_acumulado_gt,
       ROUND(participacao_co2_acumulado_mundial_pct, 1) AS participacao_acumulada_pct
FROM paises
WHERE ranking_2024 <= 10 OR ranking_historico <= 10 OR pais = 'Brazil'
ORDER BY ranking_2024
""")

# COMMAND ----------

concentracao = consultar(f"""
WITH paises AS (
    SELECT f.participacao_co2_mundial_pct AS p_ano, f.participacao_co2_acumulado_mundial_pct AS p_acum,
           RANK() OVER (ORDER BY f.co2_mt DESC) AS r_ano, RANK() OVER (ORDER BY f.co2_acumulado_mt DESC) AS r_acum
    FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.is_pais AND f.ano = {ANO_REFERENCIA} AND f.co2_mt IS NOT NULL
)
SELECT ROUND(SUM(CASE WHEN r_ano <= 3 THEN p_ano END), 1)    AS top3_2024_pct,
       ROUND(SUM(CASE WHEN r_ano <= 10 THEN p_ano END), 1)   AS top10_2024_pct,
       ROUND(SUM(CASE WHEN r_acum <= 3 THEN p_acum END), 1)  AS top3_acumulado_pct,
       ROUND(SUM(CASE WHEN r_acum <= 10 THEN p_acum END), 1) AS top10_acumulado_pct,
       COUNT(*) AS paises_com_dado
FROM paises
""")

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
top_ano = p1[p1.ranking_2024 <= 10].sort_values("ranking_2024")
barras_horizontais(ax1, top_ano.pais, top_ano.participacao_2024_pct, AZUL, "{:.1f}%",
                   f"Participação nas emissões de {ANO_REFERENCIA}", "% do CO₂ mundial")
top_hist = p1[p1.ranking_historico <= 10].sort_values("ranking_historico")
barras_horizontais(ax2, top_hist.pais, top_hist.participacao_acumulada_pct, AZUL, "{:.1f}%",
                   f"Participação nas emissões acumuladas (1750–{ANO_REFERENCIA})", "% do CO₂ acumulado mundial")
fig.suptitle("P1 · Os 10 maiores emissores de CO₂ fóssil: hoje e na história", x=0.01, ha="left", fontweight="bold", fontsize=13)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P1.** As emissões são extremamente concentradas. Em 2024, **China (31,8%), Estados Unidos (12,7%) e Índia (8,3%)
# MAGIC respondem por 52,8% do CO₂ fóssil mundial**, e os 10 maiores emissores somam 68,9%, entre 216 países com dado.
# MAGIC
# MAGIC No acumulado desde 1750 a liderança muda: **os Estados Unidos respondem sozinhos por 23,5%** de todo o CO₂ já emitido
# MAGIC (434,9 bilhões de toneladas), seguidos de China (15,4%), Rússia (6,6%), Alemanha (5,1%) e Reino Unido (4,3%). Os 10 maiores
# MAGIC concentram 68,2% do acumulado.
# MAGIC
# MAGIC As duas listas contam histórias diferentes. O **Reino Unido** é só o 18º emissor hoje, mas o 5º da história (foi o berço da
# MAGIC Revolução Industrial). A **Alemanha** cai de 4º para 10º. A **Índia** sobe de 7º na história para 3º hoje, e **Indonésia, Irã, Arábia Saudita
# MAGIC e Coreia do Sul** entram no top 10 atual sem estar entre os 10 maiores da história, por causa de um crescimento recente. Isso ajuda a
# MAGIC entender a tensão das negociações climáticas entre "quem emite hoje" e "quem emitiu para chegar aonde está". O Brasil é o 13º emissor de CO₂ fóssil em 2024 (1,3%) e o 19º no acumulado (1,0%).

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · O ranking muda quando consideramos a população? Onde está o Brasil?
# MAGIC
# MAGIC Para o ranking per capita, só entram países com pelo menos 1 milhão de habitantes (o diagnóstico mostrou que microterritórios
# MAGIC como Sint Maarten distorcem o topo da lista).

# COMMAND ----------

p2 = consultar(f"""
SELECT l.nome_localidade AS pais, ROUND(f.co2_per_capita_t, 1) AS co2_per_capita_t, ROUND(f.populacao / 1e6, 1) AS populacao_milhoes,
       RANK() OVER (ORDER BY f.co2_per_capita_t DESC) AS ranking_per_capita
FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
WHERE l.is_pais AND f.ano = {ANO_REFERENCIA} AND f.populacao >= {POPULACAO_MINIMA_RANKING_PER_CAPITA} AND f.co2_per_capita_t IS NOT NULL
ORDER BY ranking_per_capita
""")

# COMMAND ----------

posicao_brasil = consultar(f"""
WITH paises AS (
    SELECT l.nome_localidade AS pais, f.populacao,
           RANK() OVER (ORDER BY f.co2_mt DESC) AS r_total,
           RANK() OVER (ORDER BY f.co2_acumulado_mt DESC) AS r_acumulado,
           RANK() OVER (ORDER BY f.co2_incl_uso_terra_mt DESC) AS r_total_com_uso_terra,
           RANK() OVER (ORDER BY f.co2_acumulado_incl_uso_terra_mt DESC) AS r_acumulado_com_uso_terra,
           f.co2_per_capita_t
    FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.is_pais AND f.ano = {ANO_REFERENCIA} AND f.co2_mt IS NOT NULL
),
per_capita AS (
    SELECT pais, RANK() OVER (ORDER BY co2_per_capita_t DESC) AS r_per_capita, COUNT(*) OVER () AS paises_per_capita
    FROM paises WHERE populacao >= {POPULACAO_MINIMA_RANKING_PER_CAPITA}
)
SELECT p.pais, p.r_total AS ranking_co2_total, p.r_acumulado AS ranking_co2_acumulado,
       pc.r_per_capita AS ranking_per_capita, pc.paises_per_capita,
       p.r_total_com_uso_terra AS ranking_co2_com_uso_terra, p.r_acumulado_com_uso_terra AS ranking_acumulado_com_uso_terra,
       ROUND(p.co2_per_capita_t, 2) AS co2_per_capita_t,
       ROUND((SELECT co2_per_capita_t FROM {FATO_EMISSOES} JOIN {DIM_LOCALIDADE} USING (sk_localidade)
              WHERE tipo_localidade = 'Mundo' AND ano = {ANO_REFERENCIA}), 2) AS media_mundial_per_capita_t
FROM paises p JOIN per_capita pc USING (pais)
WHERE p.pais IN ('Brazil', 'China', 'United States', 'India')
ORDER BY ranking_co2_total
""")

# COMMAND ----------

media_mundial = float(posicao_brasil.media_mundial_per_capita_t.iloc[0])
top_pc = p2[p2.ranking_per_capita <= 15]
brasil_pc = p2[p2.pais == "Brazil"]
grafico = top_pc if "Brazil" in set(top_pc.pais) else pd.concat([top_pc, brasil_pc])
rotulos = [f"{p}  (#{int(r)})" for p, r in zip(grafico.pais, grafico.ranking_per_capita)]
cores = [LARANJA if p == "Brazil" else AZUL for p in grafico.pais]

fig, ax = plt.subplots(figsize=(9, 6))
barras_horizontais(ax, rotulos, grafico.co2_per_capita_t, cores, "{:.1f} t",
                   f"P2 · CO₂ per capita em {ANO_REFERENCIA}: 15 maiores e o Brasil (países com ≥ 1 milhão de hab.)",
                   "toneladas de CO₂ por pessoa")
ax.axvline(media_mundial, color=TINTA_SECUNDARIA, linewidth=1)
ax.annotate(f"média mundial: {media_mundial:.1f} t", (media_mundial, -0.7), xytext=(4, 0),
            textcoords="offset points", fontsize=8.5, color=TINTA_SECUNDARIA, va="center")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P2.** Sim, o ranking muda completamente. No CO₂ **per capita** (países com pelo menos 1 milhão de habitantes), o topo é
# MAGIC dominado por **petroestados** com população pequena e energia barata: Catar (41,3 t por pessoa), Kuwait (26,2 t), Bahrein, Trinidad
# MAGIC e Tobago, Arábia Saudita e Emirados. Entre as grandes economias, Austrália (14,5 t), EUA (14,2 t, 9º lugar), Canadá (13,4 t) e
# MAGIC Rússia (12,3 t) aparecem bem acima da média mundial de **4,7 t**. A China, 1ª no total, é apenas a 19ª per capita (8,7 t),
# MAGIC e a Índia, 3ª no total, é a 91ª (2,2 t).
# MAGIC
# MAGIC **O Brasil muda de posição conforme a métrica:**
# MAGIC
# MAGIC | Métrica (2024) | Posição do Brasil |
# MAGIC |---|---|
# MAGIC | CO₂ fóssil total | 13º |
# MAGIC | CO₂ fóssil acumulado (1750–2024) | 19º |
# MAGIC | CO₂ fóssil per capita | 88º de 160 (2,3 t, metade da média mundial) |
# MAGIC | CO₂ **incluindo mudança no uso da terra** | **5º** |
# MAGIC | CO₂ acumulado **incluindo uso da terra** | **4º** |
# MAGIC
# MAGIC Olhando só para combustíveis fósseis, o Brasil parece um emissor modesto. Quando o desmatamento entra na conta, ele salta
# MAGIC para o grupo dos cinco maiores emissores do planeta (ver P6).

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · É possível crescer sem emitir mais? (desacoplamento entre PIB e CO₂, 2000 → 2022)
# MAGIC
# MAGIC 2022 é o último ano com PIB na base (diagnóstico, seção 8). Classificação de cada país (com ≥ 1 milhão de habitantes):
# MAGIC
# MAGIC - **Desacoplamento absoluto:** PIB cresceu e CO₂ caiu;
# MAGIC - **Desacoplamento relativo:** PIB e CO₂ cresceram, mas o CO₂ cresceu menos (a economia ficou menos intensiva em carbono);
# MAGIC - **Acoplado:** o CO₂ cresceu tanto quanto ou mais que o PIB;
# MAGIC - **PIB em queda:** a economia encolheu no período.
# MAGIC
# MAGIC Também comparamos a variação das emissões **baseadas no consumo** (que incluem o CO₂ embutido em importações) para checar se a
# MAGIC queda não é apenas "exportação" da produção poluente para outros países.

# COMMAND ----------

p3 = consultar(f"""
WITH inicio AS (
    SELECT sk_localidade, pib_usd_ppc_2011 AS pib, co2_mt AS co2, co2_consumo_mt AS co2_consumo
    FROM {FATO_EMISSOES} WHERE ano = 2000
),
fim AS (
    SELECT sk_localidade, pib_usd_ppc_2011 AS pib, co2_mt AS co2, co2_consumo_mt AS co2_consumo, populacao
    FROM {FATO_EMISSOES} WHERE ano = {ANO_PIB_FINAL}
),
variacao AS (
    SELECT l.nome_localidade AS pais, fim.co2 AS co2_2022_mt,
           (fim.pib / NULLIF(inicio.pib, 0) - 1) * 100 AS var_pib_pct,
           (fim.co2 / NULLIF(inicio.co2, 0) - 1) * 100 AS var_co2_pct,
           (fim.co2_consumo / NULLIF(inicio.co2_consumo, 0) - 1) * 100 AS var_co2_consumo_pct
    FROM inicio JOIN fim USING (sk_localidade) JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.is_pais AND fim.populacao >= {POPULACAO_MINIMA_RANKING_PER_CAPITA}
      AND inicio.pib > 0 AND fim.pib > 0 AND inicio.co2 > 0 AND fim.co2 IS NOT NULL
)
SELECT pais, ROUND(co2_2022_mt, 0) AS co2_2022_mt, ROUND(var_pib_pct, 1) AS var_pib_pct, ROUND(var_co2_pct, 1) AS var_co2_pct,
       ROUND(var_co2_consumo_pct, 1) AS var_co2_consumo_pct,
       CASE WHEN var_pib_pct <= 0 THEN 'PIB em queda'
            WHEN var_co2_pct < 0 THEN 'Desacoplamento absoluto'
            WHEN var_co2_pct < var_pib_pct THEN 'Desacoplamento relativo'
            ELSE 'Acoplado' END AS classificacao,
       RANK() OVER (ORDER BY co2_2022_mt DESC) AS ranking_emissor_2022
FROM variacao
ORDER BY ranking_emissor_2022
""", visao="desacoplamento")

# COMMAND ----------

resumo_p3 = consultar(f"""
SELECT classificacao,
       COUNT(*) AS paises,
       SUM(CASE WHEN ranking_emissor_2022 <= 20 THEN 1 ELSE 0 END) AS entre_os_20_maiores_emissores,
       ROUND(100 * SUM(co2_2022_mt) / SUM(SUM(co2_2022_mt)) OVER (), 1) AS pct_do_co2_2022_do_grupo
FROM desacoplamento
GROUP BY classificacao
ORDER BY paises DESC
""")

# COMMAND ----------

display(spark.createDataFrame(p3[(p3.ranking_emissor_2022 <= 20) & (p3.classificacao == "Desacoplamento absoluto")]))

# COMMAND ----------

grandes = p3[p3.ranking_emissor_2022 <= 30]
fig, ax = plt.subplots(figsize=(9.5, 6))
outros = grandes[grandes.pais != "Brazil"]
ax.scatter(outros.var_pib_pct, outros.var_co2_pct, s=46, color=AZUL, edgecolor="#fcfcfb", linewidth=1.5, zorder=3)
brasil = grandes[grandes.pais == "Brazil"]
ax.scatter(brasil.var_pib_pct, brasil.var_co2_pct, s=64, color=LARANJA, edgecolor="#fcfcfb", linewidth=1.5, zorder=4)
ax.axhline(0, color=TINTA_SECUNDARIA, linewidth=1)
ax.axvline(0, color=TINTA_SECUNDARIA, linewidth=1)
limite = max(grandes.var_pib_pct.max(), grandes.var_co2_pct.max()) * 1.05
ax.plot([0, limite], [0, limite], color=CINZA, linewidth=1)
ax.annotate("CO₂ cresce no mesmo ritmo do PIB", (limite * 0.62, limite * 0.62), rotation=0, fontsize=8, color="#898781",
            xytext=(6, -12), textcoords="offset points")
ax.text(0.99, 0.02, "desacoplamento absoluto\n(PIB ↑, CO₂ ↓)", transform=ax.transAxes, ha="right", va="bottom", fontsize=8.5, color=TINTA_SECUNDARIA)
# rótulo seletivo (deslocamento em pontos) para não sobrepor os países agrupados no canto inferior esquerdo
DESTAQUES = {"China": (6, 2), "India": (6, 2), "United States": (6, 4), "United Kingdom": (6, -4),
             "Brazil": (7, 2), "Russia": (6, 2), "Indonesia": (6, 2), "Saudi Arabia": (6, 2), "Vietnam": (6, 2),
             "Poland": (6, 2), "Iraq": (-10, 8), "United Arab Emirates": (-40, 8)}
for r in grandes.itertuples():
    if r.pais in DESTAQUES:
        ax.annotate(r.pais, (r.var_pib_pct, r.var_co2_pct), xytext=DESTAQUES[r.pais], textcoords="offset points",
                    fontsize=8, color=TINTA_SECUNDARIA)
ax.set_xlabel(f"variação do PIB 2000–{ANO_PIB_FINAL} (%)")
ax.set_ylabel(f"variação do CO₂ 2000–{ANO_PIB_FINAL} (%)")
ax.set_title(f"P3 · PIB × CO₂ entre 2000 e {ANO_PIB_FINAL}: os 30 maiores emissores (Brasil em laranja)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P3.** Sim, é possível crescer emitindo menos, mas isso ainda não é a regra entre os maiores emissores. Entre 2000 e 2022, dos
# MAGIC 153 países analisados (≥ 1 milhão de habitantes e dados nos dois anos):
# MAGIC
# MAGIC - **40 países tiveram desacoplamento absoluto** (PIB subiu, CO₂ caiu) e respondem por 27,8% do CO₂ do grupo em 2022;
# MAGIC - **74 tiveram desacoplamento relativo** (o CO₂ cresceu, mas menos que o PIB) e respondem por 69,9%;
# MAGIC - 34 continuaram **acoplados** (CO₂ cresceu no mesmo ritmo ou mais rápido que o PIB), mas somam só 1,9% das emissões;
# MAGIC - 5 tiveram queda do PIB.
# MAGIC
# MAGIC Entre os **20 maiores emissores, 7 conseguiram o desacoplamento absoluto**: Reino Unido (PIB +38%, CO₂ −45%), Itália (+15%, −28%),
# MAGIC Alemanha (+44%, −26%), Japão (+13%, −18%), EUA (+51%, −16%), Canadá (+55%, −3%) e Polônia (+152%, −1%).
# MAGIC
# MAGIC **A queda não é só "exportação" de poluição.** Nas emissões baseadas no consumo, que incluem o CO₂ embutido nas importações,
# MAGIC 6 desses 7 países também reduziram as emissões (EUA −11%, Reino Unido −32%, Alemanha −24%). Em EUA, Reino Unido, Alemanha e Itália,
# MAGIC porém, a queda no consumo é menor que a territorial, sinal de que parte da produção migrou para fora; no Japão e no Canadá ocorreu o
# MAGIC inverso. A Polônia só se desacopla no critério territorial (consumo +2%).
# MAGIC
# MAGIC Os grandes emissores emergentes seguem em **desacoplamento relativo**: China (PIB +353%, CO₂ +221%), Índia (+274%, +187%) e
# MAGIC **Brasil (+83%, +41%)**. O Vietnã é o caso extremo de acoplamento (PIB +278%, CO₂ +498%). No mundo, o PIB cresceu 117% e o CO₂ 47%
# MAGIC no período: a economia global ficou menos intensiva em carbono, mas as emissões totais continuaram subindo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · A transição energética está acontecendo?
# MAGIC
# MAGIC Participação das fontes de baixo carbono (renováveis + nuclear) no consumo de energia primária, somando as fontes da
# MAGIC `dim_fonte_energia` com `is_baixo_carbono = true`. O mundo é a linha publicada para "World" (tipo `Mundo`).

# COMMAND ----------

p4_serie = consultar(f"""
WITH serie AS (
    SELECT fe.ano,
           SUM(CASE WHEN l.tipo_localidade = 'Mundo' AND fo.is_baixo_carbono THEN fe.participacao_primaria_pct END) AS mundo_baixo_carbono_pct,
           SUM(CASE WHEN l.tipo_localidade = 'Mundo' AND fo.is_renovavel THEN fe.participacao_primaria_pct END) AS mundo_renovaveis_pct,
           SUM(CASE WHEN l.nome_localidade = 'Brazil' AND fo.is_baixo_carbono THEN fe.participacao_primaria_pct END) AS brasil_baixo_carbono_pct
    FROM {FATO_ENERGIA} fe
    JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    JOIN {DIM_FONTE} fo USING (sk_fonte)
    WHERE l.tipo_localidade = 'Mundo' OR l.nome_localidade = 'Brazil'
    GROUP BY fe.ano
)
SELECT ano, ROUND(mundo_baixo_carbono_pct, 1) AS mundo_baixo_carbono_pct, ROUND(mundo_renovaveis_pct, 1) AS mundo_renovaveis_pct,
       ROUND(brasil_baixo_carbono_pct, 1) AS brasil_baixo_carbono_pct
FROM serie
WHERE mundo_baixo_carbono_pct IS NOT NULL
ORDER BY ano
""")

# COMMAND ----------

p4_ranking = consultar(f"""
WITH baixo_carbono AS (
    SELECT l.nome_localidade AS pais,
           SUM(CASE WHEN fo.is_baixo_carbono THEN fe.participacao_primaria_pct END) AS energia_baixo_carbono_pct,
           SUM(CASE WHEN fo.is_baixo_carbono THEN fe.participacao_eletrica_pct END) AS eletricidade_baixo_carbono_pct,
           SUM(CASE WHEN fo.subcategoria = 'Renovável' THEN fe.participacao_primaria_pct END) AS energia_renovavel_pct
    FROM {FATO_ENERGIA} fe
    JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    JOIN {DIM_FONTE} fo USING (sk_fonte)
    WHERE l.is_pais AND fe.ano = {ANO_REFERENCIA}
    GROUP BY l.nome_localidade
    HAVING COUNT(fe.participacao_primaria_pct) = 9
)
SELECT pais, ROUND(energia_baixo_carbono_pct, 1) AS energia_baixo_carbono_pct, ROUND(energia_renovavel_pct, 1) AS energia_renovavel_pct,
       ROUND(eletricidade_baixo_carbono_pct, 1) AS eletricidade_baixo_carbono_pct,
       RANK() OVER (ORDER BY energia_baixo_carbono_pct DESC) AS ranking, COUNT(*) OVER () AS paises_com_matriz_detalhada
FROM baixo_carbono
ORDER BY ranking
""")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 4.6))
for coluna, cor, rotulo in [("brasil_baixo_carbono_pct", LARANJA, "Brasil"), ("mundo_baixo_carbono_pct", AZUL, "Mundo")]:
    serie = p4_serie.dropna(subset=[coluna])
    ax.plot(serie.ano, serie[coluna], color=cor, label=rotulo)
    ultimo = serie.iloc[-1]
    ax.plot(ultimo.ano, ultimo[coluna], "o", color=cor, markersize=7, markeredgecolor="#fcfcfb", markeredgewidth=1.5)
    ax.annotate(f"{rotulo}: {ultimo[coluna]:.1f}%", (ultimo.ano, ultimo[coluna]), xytext=(7, 0), textcoords="offset points",
                va="center", fontsize=9, color=TINTA_SECUNDARIA)
ax.set_xlim(p4_serie.ano.min(), p4_serie.ano.max() + 9)
ax.set_ylim(0, 60)
ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
ax.set_ylabel("% da energia primária")
ax.set_title("P4 · Participação das fontes de baixo carbono (renováveis + nuclear) na energia primária")
ax.legend(loc="upper left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P4.** Está acontecendo, mas devagar e sem substituir os fósseis. A participação de baixo carbono na energia primária mundial
# MAGIC saiu de **6,3% em 1965** para 14,0% em 2000 e **18,7% em 2024** (renováveis 14,8% + nuclear 3,9%). A virada recente é visível: entre
# MAGIC 2000 e 2015 a participação praticamente não se moveu (14,0% → 14,3%), e desde 2015 subiu 4,4 pontos percentuais. Esse ganho foi puxado
# MAGIC por **solar e eólica, que passaram de 1,8% para 6,4%** da energia mundial.
# MAGIC
# MAGIC Os fósseis ainda são **81% da energia primária mundial** (eram 94% em 1965). Em volume absoluto, o consumo de fósseis **cresceu 51%**
# MAGIC entre 2000 e 2024: a energia limpa está sendo **somada** à matriz, não substituindo o fóssil.
# MAGIC
# MAGIC O **Brasil é um ponto fora da curva**: 50,6% da sua energia primária é de baixo carbono (49,6% renovável). É o **7º lugar entre os 79
# MAGIC países** com matriz detalhada, atrás apenas de Islândia, Suécia, Noruega, Finlândia, Suíça e França. Na eletricidade a vantagem é
# MAGIC ainda maior: **89,4% de baixo carbono, contra 40,9% no mundo**. A explicação é estrutural: hidrelétricas e bioenergia (etanol e
# MAGIC bagaço de cana), presentes desde os anos 1970–80, como mostra a curva.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Mais energia de baixo carbono significa energia menos intensiva em carbono?
# MAGIC
# MAGIC Consulta *drill-across*: cruza as duas tabelas fato pelas dimensões conformadas (país e ano). A intensidade de carbono da
# MAGIC energia (`co2_por_energia_g_kwh`) vem da `fato_emissoes_anual`; a participação de baixo carbono, da `fato_energia_fonte`.

# COMMAND ----------

p5 = consultar(f"""
WITH baixo_carbono AS (
    SELECT fe.sk_localidade, fe.ano, SUM(CASE WHEN fo.is_baixo_carbono THEN fe.participacao_primaria_pct END) AS baixo_carbono_pct
    FROM {FATO_ENERGIA} fe JOIN {DIM_FONTE} fo USING (sk_fonte)
    WHERE fe.ano = {ANO_REFERENCIA}
    GROUP BY fe.sk_localidade, fe.ano
    HAVING COUNT(fe.participacao_primaria_pct) = 9
)
SELECT l.nome_localidade AS pais, ROUND(bc.baixo_carbono_pct, 1) AS baixo_carbono_pct,
       ROUND(em.co2_por_energia_g_kwh, 0) AS co2_por_energia_g_kwh, ROUND(em.co2_per_capita_t, 1) AS co2_per_capita_t
FROM baixo_carbono bc
JOIN {FATO_EMISSOES} em USING (sk_localidade, ano)
JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
WHERE l.is_pais AND em.co2_por_energia_g_kwh IS NOT NULL
ORDER BY baixo_carbono_pct DESC
""", visao="baixo_carbono_intensidade")

# COMMAND ----------

correlacao = consultar("""
SELECT ROUND(CORR(baixo_carbono_pct, co2_por_energia_g_kwh), 2) AS correlacao_pearson, COUNT(*) AS paises
FROM baixo_carbono_intensidade
""")

# COMMAND ----------

r = float(correlacao.correlacao_pearson.iloc[0])
fig, ax = plt.subplots(figsize=(9.5, 5.6))
outros = p5[p5.pais != "Brazil"]
ax.scatter(outros.baixo_carbono_pct, outros.co2_por_energia_g_kwh, s=46, color=AZUL, edgecolor="#fcfcfb", linewidth=1.5, zorder=3)
brasil = p5[p5.pais == "Brazil"]
ax.scatter(brasil.baixo_carbono_pct, brasil.co2_por_energia_g_kwh, s=70, color=LARANJA, edgecolor="#fcfcfb", linewidth=1.5, zorder=4)
inclinacao, intercepto = np.polyfit(p5.baixo_carbono_pct, p5.co2_por_energia_g_kwh, 1)
x = np.linspace(0, p5.baixo_carbono_pct.max(), 50)
ax.plot(x, inclinacao * x + intercepto, color=CINZA, linewidth=1.5, zorder=2)
for linha in p5.itertuples():
    if linha.pais in {"Brazil", "Sweden", "France", "Iceland", "China", "India", "United States", "South Africa",
                      "Switzerland", "Saudi Arabia", "Canada", "Singapore", "Kazakhstan"}:
        ax.annotate(linha.pais, (linha.baixo_carbono_pct, linha.co2_por_energia_g_kwh), xytext=(5, 3), textcoords="offset points",
                    fontsize=8, color=TINTA_SECUNDARIA)
ax.set_xlabel(f"participação de baixo carbono na energia primária em {ANO_REFERENCIA} (%)")
ax.set_ylabel("g de CO₂ por kWh de energia primária")
ax.set_title(f"P5 · Matriz de baixo carbono × intensidade de carbono da energia ({len(p5)} países, r = {r:.2f})")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P5.** Sim. Nos 79 países com matriz detalhada, a correlação entre a participação de baixo carbono e a intensidade de carbono da
# MAGIC energia é **forte e negativa (r = −0,67)**. Países com matriz limpa, como Islândia, Suécia, Noruega, Suíça e França, emitem entre 60 e
# MAGIC 105 g de CO₂ por kWh de energia primária. Países dependentes de carvão, como Cazaquistão (333 g/kWh), África do Sul (323 g/kWh),
# MAGIC Índia (282 g/kWh) e China (251 g/kWh), estão no extremo oposto. O **Brasil emite 123 g/kWh**, exatamente sobre a linha de tendência.
# MAGIC
# MAGIC A relação, porém, não é perfeita, e as exceções são informativas:
# MAGIC
# MAGIC - **O tipo de fóssil importa:** os mais dependentes de carvão (Cazaquistão, África do Sul, Índia, China, Polônia) ficam bem acima
# MAGIC   da linha de tendência, enquanto produtores de gás como Catar, Trinidad e Tobago, Turcomenistão e Arábia Saudita ficam abaixo,
# MAGIC   porque o carvão emite muito mais CO₂ por kWh.
# MAGIC - **Limitação do indicador (qualidade de dado):** Singapura aparece com 0,5% de energia limpa e apenas 51 g/kWh. Isso acontece porque o
# MAGIC   consumo de energia do país inclui muito petróleo usado como matéria-prima petroquímica e como combustível de navios, cujas emissões
# MAGIC   não são atribuídas a ele. O indicador deve ser lido com cuidado para centros de refino e *hubs* logísticos.
# MAGIC - **Energia limpa não significa emissão per capita baixa:** a Islândia tem a energia mais limpa da amostra (61 g/kWh), mas emite
# MAGIC   9,7 t de CO₂ por pessoa, o dobro da média mundial, por causa de indústrias intensivas, como a de alumínio, e do alto consumo de
# MAGIC   energia por habitante.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6 · Qual é o peso do desmatamento nas emissões do Brasil?
# MAGIC
# MAGIC `co2_uso_terra_mt` mede as emissões líquidas por mudança no uso da terra (principalmente desmatamento). Comparamos o seu peso
# MAGIC no total (`co2_incl_uso_terra_mt`) entre o Brasil, os maiores emissores e o mundo.

# COMMAND ----------

p6_comparacao = consultar(f"""
SELECT l.nome_localidade AS localidade,
       ROUND(f.co2_mt, 0) AS co2_fossil_mt,
       ROUND(f.co2_uso_terra_mt, 0) AS co2_uso_terra_mt,
       ROUND(f.co2_incl_uso_terra_mt, 0) AS co2_total_com_uso_terra_mt,
       ROUND(100 * f.co2_uso_terra_mt / NULLIF(f.co2_incl_uso_terra_mt, 0), 1) AS peso_uso_terra_pct,
       ROUND(f.co2_acumulado_incl_uso_terra_mt / 1000, 1) AS acumulado_com_uso_terra_gt
FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
WHERE f.ano = {ANO_REFERENCIA}
  AND l.nome_localidade IN ('Brazil', 'China', 'United States', 'India', 'Russia', 'Indonesia', 'Democratic Republic of Congo', 'World')
ORDER BY co2_total_com_uso_terra_mt DESC
""")

# COMMAND ----------

p6_serie = consultar(f"""
SELECT a.decada,
       ROUND(AVG(f.co2_mt), 0) AS fossil_e_industria_mt,
       ROUND(AVG(f.co2_uso_terra_mt), 0) AS uso_da_terra_mt,
       ROUND(100 * SUM(f.co2_uso_terra_mt) / NULLIF(SUM(f.co2_incl_uso_terra_mt), 0), 1) AS peso_uso_terra_pct
FROM {FATO_EMISSOES} f
JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
JOIN {DIM_ANO} a USING (ano)
WHERE l.nome_localidade = 'Brazil' AND a.ano >= 1950
GROUP BY a.decada
ORDER BY a.decada
""")

# COMMAND ----------

anual_brasil = spark.sql(f"""
    SELECT f.ano, f.co2_mt, f.co2_uso_terra_mt
    FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.nome_localidade = 'Brazil' AND f.ano >= 1950 ORDER BY f.ano
""").toPandas()

fig, ax = plt.subplots(figsize=(10, 4.6))
for coluna, cor, rotulo in [("co2_uso_terra_mt", LARANJA, "Mudança no uso da terra"), ("co2_mt", AZUL, "Combustíveis fósseis e indústria")]:
    ax.plot(anual_brasil.ano, anual_brasil[coluna], color=cor, label=rotulo)
    ultimo = anual_brasil.iloc[-1]
    ax.plot(ultimo.ano, ultimo[coluna], "o", color=cor, markersize=7, markeredgecolor="#fcfcfb", markeredgewidth=1.5)
    ax.annotate(f"{ultimo[coluna]:,.0f} Mt", (ultimo.ano, ultimo[coluna]), xytext=(7, 0), textcoords="offset points",
                va="center", fontsize=9, color=TINTA_SECUNDARIA)
ax.set_xlim(1950, anual_brasil.ano.max() + 8)
ax.set_ylim(bottom=0)
ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
ax.set_ylabel("Mt de CO₂ por ano")
ax.set_title("P6 · Brasil: emissões de CO₂ por origem (1950–2024)")
ax.legend(loc="upper left")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P6.** Para o Brasil, **o desmatamento é o centro do problema climático**. Em 2024, as emissões por mudança no uso da terra
# MAGIC foram de **1.600 Mt de CO₂, 3,3 vezes as emissões de combustíveis fósseis e indústria** (483 Mt). Elas representam **76,8% do CO₂ total
# MAGIC do país**, contra 10,6% no mundo. Entre os grandes emissores, só a Indonésia (41,5%) chega perto. Nos EUA o peso é de 2,2%, na Índia
# MAGIC de 0,2%, e na China é negativo (−2,7%): o reflorestamento chinês faz do uso da terra um sumidouro líquido.
# MAGIC
# MAGIC Na série histórica, o uso da terra dominou as emissões brasileiras em todas as décadas desde 1950 (96% nos anos 1950, 84% nos anos
# MAGIC 2000, 71% nos 2010 e 77% nos 2020). O pico foi em **2003, com 3.000 Mt**, no auge do desmatamento na Amazônia. A queda para cerca de
# MAGIC 1.060 Mt em 2011 coincide com o período de forte redução do desmatamento (2004–2012). O movimento se inverte a partir de 2018,
# MAGIC chegando a 1.653 Mt em 2022.
# MAGIC
# MAGIC A conclusão prática é que **a queda do desmatamento entre 2003 e 2011 (cerca de −1.900 Mt/ano) foi quase quatro vezes maior do que tudo
# MAGIC o que o Brasil emite hoje com combustíveis fósseis**. Nenhuma medida no setor de energia tem esse potencial. Com o uso da terra
# MAGIC incluído, o Brasil é o 5º maior emissor anual e o 4º no acumulado (139 bilhões de t; 5,1% do total mundial).

# COMMAND ----------

# MAGIC %md
# MAGIC ## P7 · Como mudou a geografia das emissões?
# MAGIC
# MAGIC Soma das emissões dos **países** por continente (continente vindo da definição oficial de regiões da OWID, `dim_localidade.continente`).
# MAGIC Como verificação, a soma por continente é comparada com os agregados de continente publicados pela própria OWID.

# COMMAND ----------

p7 = consultar(f"""
WITH por_continente AS (
    SELECT f.ano, l.continente, SUM(f.co2_mt) AS co2_mt
    FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.is_pais AND f.ano IN (1950, 1990, {ANO_REFERENCIA}) AND l.continente <> 'Antártida'
    GROUP BY f.ano, l.continente
),
publicado AS (
    SELECT f.ano, l.continente, f.co2_mt AS co2_publicado_mt
    FROM {FATO_EMISSOES} f JOIN {DIM_LOCALIDADE} l USING (sk_localidade)
    WHERE l.tipo_localidade = 'Continente' AND f.ano IN (1950, 1990, {ANO_REFERENCIA})
)
SELECT c.ano, c.continente, ROUND(c.co2_mt, 0) AS co2_mt,
       ROUND(100 * c.co2_mt / SUM(c.co2_mt) OVER (PARTITION BY c.ano), 1) AS participacao_pct,
       ROUND(100 * (c.co2_mt / NULLIF(p.co2_publicado_mt, 0) - 1), 2) AS diferenca_vs_owid_pct
FROM por_continente c LEFT JOIN publicado p USING (ano, continente)
ORDER BY c.ano, co2_mt DESC
""")

# COMMAND ----------

ORDEM_CONTINENTES = ["Ásia", "Europa", "América do Norte", "África", "América do Sul", "Oceania"]
CORES_CONTINENTES = [AZUL, LARANJA, VERDE_AGUA, AMARELO, MAGENTA, VERDE]
anos = [1950, 1990, ANO_REFERENCIA]
fig, ax = plt.subplots(figsize=(10, 3.8))
for i, ano in enumerate(anos):
    esquerda = 0
    dados_ano = p7[p7.ano == ano].set_index("continente")
    for continente, cor in zip(ORDEM_CONTINENTES, CORES_CONTINENTES):
        valor = float(dados_ano.participacao_pct.get(continente, 0))
        ax.barh(i, valor, left=esquerda, color=cor, height=0.55, edgecolor="#fcfcfb", linewidth=2,
                label=continente if i == 0 else None)
        if valor >= 6:
            ax.text(esquerda + valor / 2, i, f"{valor:.0f}%", ha="center", va="center", fontsize=9,
                    color="#ffffff" if cor in (AZUL, VERDE) else "#0b0b0b")
        esquerda += valor
ax.set_yticks(range(len(anos)))
ax.set_yticklabels([str(a) for a in anos])
ax.invert_yaxis()
ax.set_xlim(0, 100)
ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
ax.grid(axis="y", visible=False)
ax.set_title("P7 · Participação de cada continente nas emissões de CO₂ dos países")
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=6)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta P7.** O centro das emissões migrou do Atlântico Norte para a Ásia. Em **1950, América do Norte (47%) e Europa (41%) emitiam
# MAGIC 88% do CO₂** dos países, e a Ásia apenas 7%. Em **1990** a distribuição era equilibrada: Europa 36%, Ásia 30% e América do Norte 27%.
# MAGIC Em **2024 a Ásia responde por 62,5%**, contra 16,3% da América do Norte e 13,0% da Europa.
# MAGIC
# MAGIC Não é só uma mudança de participação. Em valores absolutos, a **Europa reduziu suas emissões em 39%** desde 1990 (de 8.039 para
# MAGIC 4.881 Mt), enquanto a **Ásia multiplicou as suas por 3,6** (de 6.582 para 23.392 Mt). África (4,0%) e América do Sul (3,0%) seguem
# MAGIC com participações pequenas no CO₂ fóssil. No caso da América do Sul, o desmatamento não está nesta conta (ver P6).
# MAGIC
# MAGIC **Verificação de qualidade:** a soma dos países de cada continente bate **exatamente (diferença de 0,0%)** com os agregados de continente
# MAGIC publicados pela OWID. Isso confirma que o continente atribuído a cada país a partir do `regions.yml` está correto e que o filtro
# MAGIC `is_pais` elimina a dupla contagem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC O problema proposto tinha quatro faces, e os dados respondem a cada uma:
# MAGIC
# MAGIC 1. **Responsabilidade (P1, P7):** poucas economias concentram as emissões. Hoje o centro é a Ásia, liderada pela China; no acumulado
# MAGIC    histórico, EUA e Europa carregam a maior parte da responsabilidade. Qualquer solução depende de um grupo pequeno de países.
# MAGIC 2. **Equidade (P2):** o total e o per capita contam histórias opostas. China e Índia são enormes no total e moderadas por pessoa, e os
# MAGIC    petroestados e os EUA são o contrário. A escolha da métrica muda o ranking e o "culpado".
# MAGIC 3. **Crescimento × emissões (P3, P4, P5):** crescer emitindo menos é possível, como mostram 40 países e 7 dos 20 maiores emissores.
# MAGIC    A transição energética acelerou depois de 2015, e mais energia de baixo carbono está de fato associada a menos carbono por kWh
# MAGIC    (r = −0,67). Mesmo assim, no agregado mundial, a energia limpa ainda é somada aos fósseis em vez de substituí-los, e as emissões
# MAGIC    globais continuam subindo.
# MAGIC 4. **O caso do Brasil (P2, P4, P6):** o país tem uma das matrizes energéticas mais limpas do mundo (50,6% de baixo carbono na energia e
# MAGIC    89% na eletricidade) e emissões fósseis per capita equivalentes a metade da média mundial. Ainda assim, está entre os cinco maiores
# MAGIC    emissores do planeta por causa do desmatamento, que responde por 77% do seu CO₂. **Para o Brasil, política climática é,
# MAGIC    antes de tudo, política de uso da terra.**
# MAGIC
# MAGIC **Contribuição da engenharia de dados para as respostas.** Três decisões de pipeline foram determinantes:
# MAGIC
# MAGIC - a classificação das localidades com a flag `is_pais` evitou somas 6,4 vezes maiores que o total mundial;
# MAGIC - as dimensões conformadas da gold permitiram cruzar emissões e energia na mesma consulta (P5);
# MAGIC - a fixação das versões das fontes torna todos os números acima reproduzíveis.
# MAGIC
# MAGIC **Limitações:**
# MAGIC
# MAGIC - PIB disponível só até 2022;
# MAGIC - matriz energética detalhada para 79 países;
# MAGIC - emissões de consumo para cerca de 120 países;
# MAGIC - as emissões por uso da terra são estimativas de modelos com incerteza maior que a das emissões fósseis.
