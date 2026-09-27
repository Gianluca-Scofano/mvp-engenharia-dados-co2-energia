# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Catálogo de dados (Unity Catalog)
# MAGIC
# MAGIC O catálogo de dados é registrado **no próprio Unity Catalog**, como comentários de tabela e de coluna, visíveis no Catalog Explorer.
# MAGIC Para cada tabela e cada campo ficam documentados:
# MAGIC
# MAGIC - **descrição** do contexto da tabela e do significado de cada campo (com a unidade de medida);
# MAGIC - **tipo de dado** (vem do próprio schema da tabela);
# MAGIC - **domínio de valores**: mínimo e máximo para campos numéricos e datas, lista de categorias para campos categóricos —
# MAGIC   calculado **a partir dos dados**, para que a documentação nunca fique desatualizada em relação ao conteúdo;
# MAGIC - **linhagem**: de qual arquivo/coluna de origem o campo veio e qual transformação o gerou. A linhagem entre tabelas também é
# MAGIC   registrada automaticamente pelo Unity Catalog (aba *Lineage*).
# MAGIC
# MAGIC Tudo é consolidado na tabela `governanca.catalogo_dados`, que pode ser consultada com SQL.
# MAGIC Para a camada bronze, o dicionário oficial de cada coluna de origem está nas tabelas `bronze.owid_co2_codebook_raw` e
# MAGIC `bronze.owid_energia_codebook_raw` (codebooks publicados pela própria OWID).

# COMMAND ----------

# MAGIC %run ./00_configuracao

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, BooleanType, TimestampType, DateType

BRONZE_CO2 = "bronze.owid_co2_raw"
BRONZE_ENERGIA = "bronze.owid_energia_raw"
BRONZE_REGIOES = "bronze.owid_regioes_raw"
CONTROLE = {
    "_versao_origem": ("Commit do repositório da OWID de onde o arquivo foi baixado (versão exata do dado).", "bronze._versao_origem"),
    "_data_processamento": ("Data e hora em que a linha foi gravada na camada silver.", "gerado no notebook 04"),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Documentação das tabelas e colunas
# MAGIC
# MAGIC Cada coluna recebe `(descrição, origem)`. As medidas de emissões e as medidas por fonte de energia são descritas uma única vez
# MAGIC e reaproveitadas na silver e na gold.

# COMMAND ----------

# Medidas anuais de emissões: nome na silver/gold → (descrição, coluna de origem em bronze.owid_co2_raw)
MEDIDAS_EMISSOES = {
    "populacao": ("População total (pessoas).", "population"),
    "pib_usd_ppc_2011": ("PIB em dólares internacionais a preços de 2011 (ajustado por inflação e paridade de poder de compra); disponível até 2022.", "gdp"),
    "co2_mt": ("Emissões anuais de CO2 de combustíveis fósseis e indústria, sem mudança no uso da terra, em milhões de toneladas (Mt).", "co2"),
    "co2_carvao_mt": ("Emissões de CO2 da queima de carvão (Mt).", "coal_co2"),
    "co2_petroleo_mt": ("Emissões de CO2 da queima de petróleo (Mt).", "oil_co2"),
    "co2_gas_mt": ("Emissões de CO2 da queima de gás natural (Mt).", "gas_co2"),
    "co2_cimento_mt": ("Emissões de CO2 da produção de cimento (Mt).", "cement_co2"),
    "co2_flaring_mt": ("Emissões de CO2 da queima de gás em flare na produção de petróleo e gás (Mt).", "flaring_co2"),
    "co2_outras_industrias_mt": ("Emissões de CO2 de outros processos industriais (Mt).", "other_industry_co2"),
    "co2_uso_terra_mt": ("Emissões líquidas de CO2 por mudança no uso da terra, como desmatamento (Mt); negativo indica sumidouro de carbono.", "land_use_change_co2"),
    "co2_incl_uso_terra_mt": ("Emissões totais de CO2 incluindo mudança no uso da terra (Mt).", "co2_including_luc"),
    "co2_acumulado_mt": ("Emissões acumuladas de CO2, sem uso da terra, desde o primeiro ano com dados (Mt).", "cumulative_co2"),
    "co2_acumulado_incl_uso_terra_mt": ("Emissões acumuladas de CO2 incluindo uso da terra (Mt).", "cumulative_co2_including_luc"),
    "co2_per_capita_t": ("Emissões de CO2 por pessoa, sem uso da terra (toneladas por pessoa).", "co2_per_capita"),
    "co2_por_pib_kg_por_usd": ("Intensidade de carbono da economia: kg de CO2 por dólar internacional de PIB.", "co2_per_gdp"),
    "co2_consumo_mt": ("Emissões baseadas no consumo, ajustadas pelo comércio internacional (Mt).", "consumption_co2"),
    "co2_comercio_liquido_mt": ("CO2 líquido embutido no comércio (Mt); negativo indica exportador líquido de emissões.", "trade_co2"),
    "energia_primaria_twh": ("Consumo de energia primária (TWh).", "primary_energy_consumption"),
    "energia_per_capita_kwh": ("Consumo de energia primária por pessoa (kWh).", "energy_per_capita"),
    "co2_por_energia_g_kwh": ("Intensidade de carbono da energia: gramas de CO2 por kWh de energia primária.", "co2_per_unit_energy"),
    "participacao_co2_mundial_pct": ("Participação nas emissões mundiais de CO2 do ano (%).", "share_global_co2"),
    "participacao_co2_acumulado_mundial_pct": ("Participação nas emissões mundiais acumuladas de CO2 (%).", "share_global_cumulative_co2"),
    "gee_total_mt_co2e": ("Emissões totais de gases de efeito estufa, incluindo uso da terra (Mt de CO2 equivalente, horizonte de 100 anos).", "total_ghg"),
    "metano_mt_co2e": ("Emissões de metano (Mt de CO2 equivalente).", "methane"),
    "oxido_nitroso_mt_co2e": ("Emissões de óxido nitroso (Mt de CO2 equivalente).", "nitrous_oxide"),
}

# Fontes de energia: nome na silver → (nome legível, código OWID das colunas)
FONTES = {
    "carvao": ("carvão", "coal"), "petroleo": ("petróleo", "oil"), "gas_natural": ("gás natural", "gas"),
    "nuclear": ("energia nuclear", "nuclear"), "hidreletrica": ("hidrelétrica", "hydro"), "solar": ("solar", "solar"),
    "eolica": ("eólica", "wind"), "biocombustiveis": ("biocombustíveis e bioenergia", "biofuel"),
    "outras_renovaveis": ("outras renováveis (geotérmica, marés etc., sem bioenergia)", "other_renewable"),
}


def colunas_energia_por_fonte():
    colunas = {}
    for fonte, (nome, owid) in FONTES.items():
        plural = "other_renewables" if owid == "other_renewable" else owid
        eletricidade = ("other_renewable_exc_biofuel_electricity (ou other_renewable_electricity − biofuel_electricity)"
                        if owid == "other_renewable" else f"{owid}_electricity")
        participacao_eletricidade = ("other_renewables_share_elec_exc_biofuel (ou other_renewables_share_elec − biofuel_share_elec)"
                                     if owid == "other_renewable" else f"{owid}_share_elec")
        colunas[f"consumo_{fonte}_twh"] = (f"Consumo de energia primária de {nome} (TWh, método de substituição).", f"{BRONZE_ENERGIA}.{owid}_consumption")
        colunas[f"participacao_{fonte}_pct"] = (f"Participação de {nome} no consumo de energia primária (%).", f"{BRONZE_ENERGIA}.{plural}_share_energy")
        colunas[f"eletricidade_{fonte}_twh"] = (f"Geração de eletricidade a partir de {nome} (TWh).", f"{BRONZE_ENERGIA}.{eletricidade}")
        colunas[f"participacao_eletricidade_{fonte}_pct"] = (f"Participação de {nome} na geração de eletricidade (%).", f"{BRONZE_ENERGIA}.{participacao_eletricidade}")
    return colunas


LOCALIDADE_SILVER = {
    "nome_localidade": ("Nome da localidade como publicado pela OWID (em inglês). Chave natural.", "bronze.*.country"),
    "codigo_localidade": ("Código ISO 3166-1 alfa-3 do país ou, na falta dele, código da OWID (ex.: OWID_KOS, OWID_USS, OWID_WRL).", "iso_code; senão regions.yml (busca por nome e nomes alternativos)"),
    "tipo_localidade": ("Classificação da localidade.", "regra de classificação do notebook 04"),
}

DOCUMENTACAO = {
    # ------------------------------------------------------------------ SILVER
    f"{SCHEMA_SILVER}.regioes_owid": {
        "descricao": "Referência oficial de regiões da OWID (regions.yml): códigos, países históricos e continente de cada membro. Uma linha por região.",
        "colunas": {
            "codigo_regiao": ("Código da região (ISO alfa-3 para países ou código OWID_*). Chave.", f"{BRONZE_REGIOES}.code"),
            "nome_regiao": ("Nome da região.", f"{BRONZE_REGIOES}.name"),
            "tipo_regiao_owid": ("Tipo da região segundo a OWID.", f"{BRONZE_REGIOES}.region_type (nulo = country)"),
            "is_historico": ("Verdadeiro se a região não existe mais (ex.: URSS).", f"{BRONZE_REGIOES}.is_historical"),
            "ano_fim_existencia": ("Último ano de existência de uma região histórica.", f"{BRONZE_REGIOES}.end_year"),
            "nomes_alternativos": ("Nomes alternativos usados para encontrar a região pelo nome.", f"{BRONZE_REGIOES}.aliases"),
            "continente": ("Continente (em português) ao qual a região pertence.", f"{BRONZE_REGIOES}.members dos 6 continentes (explode)"),
        },
    },
    f"{SCHEMA_SILVER}.localidades": {
        "descricao": "Todas as localidades das duas bases, com código, tipo, continente e a marcação dos agregados descartados. Uma linha por localidade.",
        "colunas": {
            **LOCALIDADE_SILVER,
            "codigo_iso": ("Código ISO 3166-1 alfa-3 como veio da origem (nulo para agregados e países históricos).", "bronze.*.iso_code"),
            "is_pais": ("Verdadeiro para países atuais: use para rankings e somas sem dupla contagem.", "tipo_localidade = 'País'"),
            "is_historico": ("Verdadeiro para países que não existem mais.", f"silver.regioes_owid.is_historico"),
            "ano_fim_existencia": ("Último ano de existência de um país histórico.", "silver.regioes_owid.ano_fim_existencia"),
            "continente": ("Continente em português (Antártida atribuída manualmente).", "silver.regioes_owid.continente"),
            "presente_em_co2": ("Verdadeiro se a localidade aparece no arquivo de CO2.", BRONZE_CO2),
            "presente_em_energia": ("Verdadeiro se a localidade aparece no arquivo de energia.", BRONZE_ENERGIA),
            "descartada": ("Verdadeiro para agregados de fontes específicas, que não seguem para as tabelas de fatos.", "tipo_localidade = 'Agregado de fonte específica'"),
            "motivo_descarte": ("Motivo do descarte.", "regra de classificação do notebook 04"),
        },
    },
    f"{SCHEMA_SILVER}.emissoes_co2": {
        "descricao": "Emissões anuais de CO2 e outros gases por localidade, limpas e tipadas, sem agregados de fontes específicas. Uma linha por localidade e ano.",
        "colunas": {
            **LOCALIDADE_SILVER,
            "ano": ("Ano de referência.", f"{BRONZE_CO2}.year (texto → inteiro)"),
            **{c: (d, f"{BRONZE_CO2}.{o} (texto → número)") for c, (d, o) in MEDIDAS_EMISSOES.items()},
        },
    },
    f"{SCHEMA_SILVER}.energia": {
        "descricao": "Consumo de energia primária e geração de eletricidade por fonte e localidade, limpos e tipados. Uma linha por localidade e ano; 4 colunas por fonte.",
        "colunas": {
            **LOCALIDADE_SILVER,
            "ano": ("Ano de referência.", f"{BRONZE_ENERGIA}.year (texto → inteiro)"),
            **colunas_energia_por_fonte(),
            "energia_primaria_twh": ("Consumo total de energia primária (TWh).", f"{BRONZE_ENERGIA}.primary_energy_consumption"),
            "participacao_baixo_carbono_pct": ("Participação das fontes de baixo carbono (renováveis + nuclear) na energia primária (%).", f"{BRONZE_ENERGIA}.low_carbon_share_energy"),
            "participacao_renovaveis_pct": ("Participação das renováveis na energia primária (%).", f"{BRONZE_ENERGIA}.renewables_share_energy"),
            "participacao_fosseis_pct": ("Participação dos combustíveis fósseis na energia primária (%).", f"{BRONZE_ENERGIA}.fossil_share_energy"),
            "geracao_eletrica_twh": ("Geração total de eletricidade (TWh).", f"{BRONZE_ENERGIA}.electricity_generation"),
            "participacao_eletricidade_baixo_carbono_pct": ("Participação das fontes de baixo carbono na geração de eletricidade (%).", f"{BRONZE_ENERGIA}.low_carbon_share_elec"),
            "intensidade_carbono_eletricidade_g_kwh": ("Gases de efeito estufa emitidos por kWh de eletricidade gerada (g CO2e/kWh).", f"{BRONZE_ENERGIA}.carbon_intensity_elec"),
            "eletricidade_abertura_completa": ("Verdadeiro se a soma da geração por fonte fecha com a geração total (± 1%).", "calculado no notebook 04"),
        },
    },
    # ------------------------------------------------------------------ GOLD
    f"{SCHEMA_GOLD}.dim_localidade": {
        "descricao": "Dimensão de localidades: países atuais, países históricos, continentes, Mundo, grupos de renda, UE e transporte internacional. Uma linha por localidade. Para somas e rankings use is_pais = true.",
        "colunas": {
            "sk_localidade": ("Chave substituta da localidade (PK), gerada por ordem alfabética do nome.", "row_number() sobre silver.localidades.nome_localidade"),
            "codigo_localidade": LOCALIDADE_SILVER["codigo_localidade"][:1] + ("silver.localidades.codigo_localidade",),
            "nome_localidade": ("Nome da localidade (em inglês, como publicado pela OWID). Chave natural.", "silver.localidades.nome_localidade"),
            "tipo_localidade": ("Classificação da localidade.", "silver.localidades.tipo_localidade"),
            "is_pais": ("Verdadeiro para países atuais: filtro obrigatório para somas e rankings sem dupla contagem.", "silver.localidades.is_pais"),
            "is_historico": ("Verdadeiro para países que não existem mais.", "silver.localidades.is_historico"),
            "ano_fim_existencia": ("Último ano de existência de um país histórico.", "silver.localidades.ano_fim_existencia"),
            "continente": ("Continente em português.", "silver.localidades.continente"),
            "presente_em_co2": ("Verdadeiro se a localidade aparece no arquivo de CO2.", "silver.localidades.presente_em_co2"),
            "presente_em_energia": ("Verdadeiro se a localidade aparece no arquivo de energia.", "silver.localidades.presente_em_energia"),
        },
    },
    f"{SCHEMA_GOLD}.dim_ano": {
        "descricao": "Dimensão de tempo com granularidade anual, do primeiro ao último ano presente nas bases. Uma linha por ano.",
        "colunas": {
            "ano": ("Ano (PK).", "sequência entre o menor e o maior ano de silver.emissoes_co2 e silver.energia"),
            "decada": ("Década do ano (ex.: 1990).", "floor(ano / 10) * 10"),
            "seculo": ("Século do ano (ex.: 20).", "floor((ano - 1) / 100) + 1"),
            "periodo_acordos_climaticos": ("Período em relação aos marcos climáticos: Protocolo de Kyoto (1997) e Acordo de Paris (2015).", "regra sobre o ano"),
        },
    },
    f"{SCHEMA_GOLD}.dim_fonte_energia": {
        "descricao": "Dimensão das 9 fontes de energia, com a classificação da OWID: baixo carbono = renováveis + nuclear. Uma linha por fonte.",
        "colunas": {
            "sk_fonte": ("Chave da fonte (PK), também define a ordem de exibição.", "tabela de referência (notebook 05)"),
            "codigo_fonte": ("Código da fonte em português, usado nos nomes de coluna da silver.", "tabela de referência"),
            "codigo_fonte_owid": ("Prefixo usado pela OWID nas colunas da fonte (coal, oil, hydro...).", "tabela de referência"),
            "nome_fonte": ("Nome da fonte para exibição.", "tabela de referência"),
            "categoria": ("Fóssil ou Baixo carbono.", "tabela de referência"),
            "subcategoria": ("Fóssil, Nuclear ou Renovável.", "tabela de referência"),
            "is_renovavel": ("Verdadeiro para fontes renováveis.", "tabela de referência"),
            "is_baixo_carbono": ("Verdadeiro para fontes de baixo carbono (renováveis + nuclear).", "tabela de referência"),
        },
    },
    f"{SCHEMA_GOLD}.fato_emissoes_anual": {
        "descricao": "Fato de emissões: uma linha por localidade e ano, com população, PIB, CO2 por origem, uso da terra, métricas per capita/intensidade e outros gases.",
        "colunas": {
            "sk_localidade": ("Chave da localidade (FK → dim_localidade).", "dim_localidade.sk_localidade via nome_localidade"),
            "ano": ("Ano (FK → dim_ano).", "silver.emissoes_co2.ano"),
            **{c: (d, f"silver.emissoes_co2.{c} ← {BRONZE_CO2}.{o}") for c, (d, o) in MEDIDAS_EMISSOES.items()},
        },
    },
    f"{SCHEMA_GOLD}.fato_energia_fonte": {
        "descricao": "Fato de energia: uma linha por localidade, ano e fonte, com consumo de energia primária e geração de eletricidade. Colunas da silver transformadas em linhas (stack).",
        "colunas": {
            "sk_localidade": ("Chave da localidade (FK → dim_localidade).", "dim_localidade.sk_localidade via nome_localidade"),
            "ano": ("Ano (FK → dim_ano).", "silver.energia.ano"),
            "sk_fonte": ("Chave da fonte de energia (FK → dim_fonte_energia).", "dim_fonte_energia via codigo_fonte"),
            "consumo_primario_twh": ("Consumo de energia primária da fonte (TWh, método de substituição).", "silver.energia.consumo_<fonte>_twh (stack)"),
            "participacao_primaria_pct": ("Participação da fonte no consumo total de energia primária (%).", "silver.energia.participacao_<fonte>_pct (stack)"),
            "geracao_eletrica_twh": ("Geração de eletricidade da fonte (TWh); nula quando a abertura por fonte não fecha com o total.", "silver.energia.eletricidade_<fonte>_twh (stack + regra de qualidade)"),
            "participacao_eletrica_pct": ("Participação da fonte na geração de eletricidade (%); nula quando a abertura não fecha com o total.", "silver.energia.participacao_eletricidade_<fonte>_pct (stack + regra de qualidade)"),
        },
    },
    # ------------------------------------------------------------------ GOVERNANÇA
    f"{SCHEMA_GOVERNANCA}.controle_ingestao": {
        "descricao": "Log de auditoria da ingestão (append): uma linha por arquivo por execução do notebook 02.",
        "colunas": {
            "data_ingestao": ("Data e hora da execução da ingestão (UTC).", "notebook 02"),
            "arquivo": ("Nome do arquivo de origem.", "configuração (00)"),
            "url_origem": ("URL oficial (fixada em um commit) de onde o arquivo foi baixado.", "configuração (00)"),
            "versao_origem": ("Commit do repositório da OWID.", "configuração (00)"),
            "sha256": ("Hash SHA-256 calculado sobre o arquivo recebido.", "notebook 02"),
            "sha256_confere": ("Verdadeiro se o hash confere com o da versão fixada.", "notebook 02"),
            "tamanho_bytes": ("Tamanho do arquivo em bytes.", "notebook 02"),
            "modo_coleta": ("Como o arquivo chegou ao Volume (download automático ou reaproveitado/upload manual).", "notebook 02"),
            "tabela_destino": ("Tabela bronze gerada a partir do arquivo.", "notebook 02"),
            "linhas_carregadas": ("Quantidade de linhas gravadas na tabela bronze.", "notebook 02"),
        },
    },
    f"{SCHEMA_GOVERNANCA}.resultados_qualidade": {
        "descricao": "Resultados das verificações de qualidade (append): diagnóstico da bronze (03) e testes pós-carga da silver (04) e da gold (05).",
        "colunas": {
            "data_execucao": ("Data e hora da execução (UTC).", "notebooks 03, 04 e 05"),
            "etapa": ("Notebook/camada que executou a verificação.", "notebooks 03, 04 e 05"),
            "dimensao": ("Dimensão de qualidade: Completude, Consistência, Unicidade, Acurácia, Outliers ou Integridade.", "notebooks 03, 04 e 05"),
            "tabela": ("Tabela verificada.", "notebooks 03, 04 e 05"),
            "verificacao": ("Descrição da verificação.", "notebooks 03, 04 e 05"),
            "valor_encontrado": ("Valor medido (contagem, percentual ou desvio).", "notebooks 03, 04 e 05"),
            "esperado": ("Valor esperado.", "notebooks 03, 04 e 05"),
            "status": ("OK, ALERTA (problema conhecido e tratado) ou FALHA.", "notebooks 03, 04 e 05"),
            "observacao": ("Contexto do resultado e tratamento aplicado.", "notebooks 03, 04 e 05"),
        },
    },
}

DESCRICOES_BRONZE = {
    f"{SCHEMA_BRONZE}.owid_co2_raw": "Arquivo owid-co2-data.csv exatamente como recebido (todas as colunas como texto) + metadados de ingestão. Colunas documentadas em bronze.owid_co2_codebook_raw.",
    f"{SCHEMA_BRONZE}.owid_energia_raw": "Arquivo owid-energy-data.csv exatamente como recebido (todas as colunas como texto) + metadados de ingestão. Colunas documentadas em bronze.owid_energia_codebook_raw.",
    f"{SCHEMA_BRONZE}.owid_regioes_raw": "Arquivo regions.yml (definição de regiões da OWID) convertido para tabela sem interpretar valores + metadados de ingestão.",
    f"{SCHEMA_BRONZE}.owid_co2_codebook_raw": "Dicionário de dados oficial do dataset de CO2 da OWID (coluna, título, descrição, unidade, fonte).",
    f"{SCHEMA_BRONZE}.owid_energia_codebook_raw": "Dicionário de dados oficial do dataset de energia da OWID (coluna, título, descrição, unidade, fonte).",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Domínio de valores calculado a partir dos dados

# COMMAND ----------

LIMITE_CATEGORIAS = 12


def formatar(valor):
    """Números no padrão brasileiro (1.234,56); inteiros pequenos, como anos, sem separador de milhar."""
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return str(valor)
    if isinstance(valor, int) and abs(valor) < 10000:
        texto = str(valor)
    elif isinstance(valor, int) or abs(valor) >= 1000:
        texto = f"{valor:,.0f}"
    elif abs(valor) >= 1:
        texto = f"{valor:,.2f}"
    else:
        texto = f"{valor:.3g}"
    return texto.replace(",", "_").replace(".", ",").replace("_", ".")


def dominios(nome_tabela):
    """Mínimo e máximo para numéricos e datas; lista de categorias para textos e booleanos com poucas categorias."""
    df = spark.table(tabela(*nome_tabela.split(".")))
    campos = df.schema.fields
    ordenaveis = [c.name for c in campos if isinstance(c.dataType, (NumericType, TimestampType, DateType))]
    categoricos = [c.name for c in campos if isinstance(c.dataType, (StringType, BooleanType))]
    agregados = [F.min(c).alias(f"min__{c}") for c in ordenaveis] + [F.max(c).alias(f"max__{c}") for c in ordenaveis] \
        + [F.countDistinct(c).alias(f"distintos__{c}") for c in categoricos] \
        + [F.avg(F.col(c.name).isNull().cast("int")).alias(f"nulos__{c.name}") for c in campos] + [F.count("*").alias("linhas")]
    linha = df.agg(*agregados).first()
    resultado = {}
    for c in ordenaveis:
        minimo, maximo = linha[f"min__{c}"], linha[f"max__{c}"]
        resultado[c] = "sem valores" if minimo is None else f"{formatar(minimo)} a {formatar(maximo)}"
    for c in categoricos:
        distintos = linha[f"distintos__{c}"]
        if distintos <= LIMITE_CATEGORIAS:
            valores = [r[0] for r in df.select(c).where(F.col(c).isNotNull()).distinct().orderBy(c).collect()]
            resultado[c] = "{" + ", ".join(str(v).lower() if isinstance(v, bool) else str(v) for v in valores) + "}"
        else:
            exemplos = [r[0] for r in df.select(c).where(F.col(c).isNotNull()).distinct().orderBy(c).limit(3).collect()]
            resultado[c] = f"{distintos} valores distintos (ex.: {', '.join(map(str, exemplos))})"
    for c in campos:
        if c.name not in resultado:
            resultado[c.name] = "lista de textos"
    pct_nulos = {c.name: float(round(100 * (linha[f"nulos__{c.name}"] or 0.0), 1)) for c in campos}
    tipos = {c.name: c.dataType.simpleString() for c in campos}
    return resultado, pct_nulos, tipos, linha["linhas"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicação dos comentários no Unity Catalog e montagem do catálogo

# COMMAND ----------

def sql_texto(texto):
    return "'" + texto.replace("\\", "\\\\").replace("'", "\\'") + "'"


linhas_catalogo = []
for nome_tabela, doc in DOCUMENTACAO.items():
    destino = tabela(*nome_tabela.split("."))
    dominio, pct_nulos, tipos, total_linhas = dominios(nome_tabela)
    spark.sql(f"COMMENT ON TABLE {destino} IS {sql_texto(doc['descricao'])}")
    colunas_doc = {**doc["colunas"], **CONTROLE}
    for coluna, tipo in tipos.items():
        descricao, origem = colunas_doc.get(coluna, ("(sem documentação)", "-"))
        comentario = f"{descricao} Domínio: {dominio[coluna]}. Origem: {origem}."
        spark.sql(f"ALTER TABLE {destino} ALTER COLUMN {coluna} COMMENT {sql_texto(comentario)}")
        linhas_catalogo.append((nome_tabela.split(".")[0], nome_tabela, doc["descricao"], total_linhas, coluna, tipo,
                                descricao, dominio[coluna], origem, pct_nulos[coluna]))
    print(f"{nome_tabela}: {len(tipos)} colunas documentadas")

for nome_tabela, descricao in DESCRICOES_BRONZE.items():
    spark.sql(f"COMMENT ON TABLE {tabela(*nome_tabela.split('.'))} IS {sql_texto(descricao)}")

catalogo = spark.createDataFrame(
    linhas_catalogo,
    "camada string, tabela string, descricao_tabela string, linhas_tabela long, coluna string, tipo string, "
    "descricao string, dominio string, origem string, pct_nulos double",
)
(catalogo.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(tabela(SCHEMA_GOVERNANCA, "catalogo_dados")))
spark.sql(f"COMMENT ON TABLE {tabela(SCHEMA_GOVERNANCA, 'catalogo_dados')} IS "
          "'Catálogo de dados consolidado: uma linha por coluna das tabelas silver, gold e de governança, com tipo, descrição, domínio e origem.'")

sem_documentacao = catalogo.where("descricao = '(sem documentação)'").count()
print(f"Colunas documentadas: {catalogo.count()} | sem documentação: {sem_documentacao}")
assert sem_documentacao == 0, "Existem colunas sem documentação no catálogo"

# COMMAND ----------

display(catalogo.groupBy("camada", "tabela").agg(F.count("*").alias("colunas"), F.first("linhas_tabela").alias("linhas")).orderBy("camada", "tabela"))

# COMMAND ----------

display(catalogo.where("camada = 'gold'").select("tabela", "coluna", "tipo", "descricao", "dominio", "origem"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Evidência: comentários gravados no Unity Catalog

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {tabela(SCHEMA_GOLD, 'fato_emissoes_anual')}"))
