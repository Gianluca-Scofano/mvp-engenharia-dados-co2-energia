# Catálogo de dados

Catálogo transcrito do Unity Catalog. Ele é gerado automaticamente pelo notebook [`06_catalogo_dados`](../notebooks/06_catalogo_dados.py), que grava cada descrição como comentário de tabela/coluna no Unity Catalog e consolida tudo na tabela `governanca.catalogo_dados`. O **domínio** de cada campo é calculado a partir dos próprios dados (mínimo–máximo para números e datas; lista de categorias para campos categóricos). A coluna **Origem** descreve a linhagem: de qual tabela/coluna o campo veio e qual transformação o gerou.

Na camada bronze, as tabelas guardam os arquivos exatamente como recebidos (todas as colunas como texto). O dicionário oficial de cada coluna de origem está nas tabelas `bronze.owid_co2_codebook_raw` e `bronze.owid_energia_codebook_raw` (codebooks da OWID).

> As contagens de linhas das tabelas de governança (logs em modo *append*) e os valores de data/hora das colunas de controle refletem a execução usada para gerar este documento; os demais domínios são determinísticos, porque as fontes estão fixadas por commit.

## Sumário

**Camada Gold (modelo dimensional)**

- [`gold.dim_ano`](#golddim_ano): 276 linhas
- [`gold.dim_fonte_energia`](#golddim_fonte_energia): 9 linhas
- [`gold.dim_localidade`](#golddim_localidade): 255 linhas
- [`gold.fato_emissoes_anual`](#goldfato_emissoes_anual): 45.267 linhas
- [`gold.fato_energia_fonte`](#goldfato_energia_fonte): 78.154 linhas

**Camada Silver (dados limpos)**

- [`silver.emissoes_co2`](#silveremissoes_co2): 45.267 linhas
- [`silver.energia`](#silverenergia): 11.732 linhas
- [`silver.localidades`](#silverlocalidades): 349 linhas
- [`silver.regioes_owid`](#silverregioes_owid): 476 linhas

**Governança (metadados do pipeline)**

- [`governanca.controle_ingestao`](#governancacontrole_ingestao): 5 linhas
- [`governanca.resultados_qualidade`](#governancaresultados_qualidade): 132 linhas

## Camada Gold (modelo dimensional)

### gold.dim_ano

Dimensão de tempo com granularidade anual, do primeiro ao último ano presente nas bases. Uma linha por ano.

**Linhas:** 276

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `ano` | int | Ano (PK). | 1750 a 2025 | sequência entre o menor e o maior ano de silver.emissoes_co2 e silver.energia | 0,0 |
| `decada` | int | Década do ano (ex.: 1990). | 1750 a 2020 | floor(ano / 10) * 10 | 0,0 |
| `seculo` | int | Século do ano (ex.: 20). | 18 a 21 | floor((ano - 1) / 100) + 1 | 0,0 |
| `periodo_acordos_climaticos` | string | Período em relação aos marcos climáticos: Protocolo de Kyoto (1997) e Acordo de Paris (2015). | {1. Até 1949, 2. 1950–1997 (antes de Kyoto), 3. 1998–2015 (Protocolo de Kyoto), 4. 2016 em diante (Acordo de Paris)} | regra sobre o ano | 0,0 |

### gold.dim_fonte_energia

Dimensão das 9 fontes de energia, com a classificação da OWID: baixo carbono = renováveis + nuclear. Uma linha por fonte.

**Linhas:** 9

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `sk_fonte` | int | Chave da fonte (PK), também define a ordem de exibição. | 1 a 9 | tabela de referência (notebook 05) | 0,0 |
| `codigo_fonte` | string | Código da fonte em português, usado nos nomes de coluna da silver. | {biocombustiveis, carvao, eolica, gas_natural, hidreletrica, nuclear, outras_renovaveis, petroleo, solar} | tabela de referência | 0,0 |
| `codigo_fonte_owid` | string | Prefixo usado pela OWID nas colunas da fonte (coal, oil, hydro...). | {biofuel, coal, gas, hydro, nuclear, oil, other_renewable, solar, wind} | tabela de referência | 0,0 |
| `nome_fonte` | string | Nome da fonte para exibição. | {Biocombustíveis e bioenergia, Carvão, Eólica, Gás natural, Hidrelétrica, Nuclear, Outras renováveis (geotérmica, marés...), Petróleo, Solar} | tabela de referência | 0,0 |
| `categoria` | string | Fóssil ou Baixo carbono. | {Baixo carbono, Fóssil} | tabela de referência | 0,0 |
| `subcategoria` | string | Fóssil, Nuclear ou Renovável. | {Fóssil, Nuclear, Renovável} | tabela de referência | 0,0 |
| `is_renovavel` | boolean | Verdadeiro para fontes renováveis. | {false, true} | tabela de referência | 0,0 |
| `is_baixo_carbono` | boolean | Verdadeiro para fontes de baixo carbono (renováveis + nuclear). | {false, true} | tabela de referência | 0,0 |

### gold.dim_localidade

Dimensão de localidades: países atuais, países históricos, continentes, Mundo, grupos de renda, UE e transporte internacional. Uma linha por localidade. Para somas e rankings use is_pais = true.

**Linhas:** 255

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `sk_localidade` | int | Chave substituta da localidade (PK), gerada por ordem alfabética do nome. | 1 a 255 | row_number() sobre silver.localidades.nome_localidade | 0,0 |
| `codigo_localidade` | string | Código ISO 3166-1 alfa-3 do país ou, na falta dele, código da OWID (ex.: OWID_KOS, OWID_USS, OWID_WRL). | 247 valores distintos (ex.: ABW, AFG, AGO) | silver.localidades.codigo_localidade | 3,1 |
| `nome_localidade` | string | Nome da localidade (em inglês, como publicado pela OWID). Chave natural. | 255 valores distintos (ex.: Afghanistan, Africa, Albania) | silver.localidades.nome_localidade | 0,0 |
| `tipo_localidade` | string | Classificação da localidade. | {Bloco econômico, Continente, Grupo de renda, Mundo, Outro, País, País histórico, Transporte internacional} | silver.localidades.tipo_localidade | 0,0 |
| `is_pais` | boolean | Verdadeiro para países atuais: filtro obrigatório para somas e rankings sem dupla contagem. | {false, true} | silver.localidades.is_pais | 0,0 |
| `is_historico` | boolean | Verdadeiro para países que não existem mais. | {false, true} | silver.localidades.is_historico | 0,0 |
| `ano_fim_existencia` | int | Último ano de existência de um país histórico. | 1990 a 2010 | silver.localidades.ano_fim_existencia | 97,3 |
| `continente` | string | Continente em português. | {América do Norte, América do Sul, Antártida, Europa, Oceania, África, Ásia} | silver.localidades.continente | 3,9 |
| `presente_em_co2` | boolean | Verdadeiro se a localidade aparece no arquivo de CO2. | {false, true} | silver.localidades.presente_em_co2 | 0,0 |
| `presente_em_energia` | boolean | Verdadeiro se a localidade aparece no arquivo de energia. | {false, true} | silver.localidades.presente_em_energia | 0,0 |

### gold.fato_emissoes_anual

Fato de emissões: uma linha por localidade e ano, com população, PIB, CO2 por origem, uso da terra, métricas per capita/intensidade e outros gases.

**Linhas:** 45.267

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `co2_cimento_mt` | double | Emissões de CO2 da produção de cimento (Mt). | 0 a 1.667 | silver.emissoes_co2.co2_cimento_mt ← bronze.owid_co2_raw.cement_co2 | 38,6 |
| `co2_flaring_mt` | double | Emissões de CO2 da queima de gás em flare na produção de petróleo e gás (Mt). | 0 a 432,39 | silver.emissoes_co2.co2_flaring_mt ← bronze.owid_co2_raw.flaring_co2 | 66,8 |
| `co2_outras_industrias_mt` | double | Emissões de CO2 de outros processos industriais (Mt). | 0 a 458,18 | silver.emissoes_co2.co2_outras_industrias_mt ← bronze.owid_co2_raw.other_industry_co2 | 93,2 |
| `co2_uso_terra_mt` | double | Emissões líquidas de CO2 por mudança no uso da terra, como desmatamento (Mt); negativo indica sumidouro de carbono. | -319,42 a 8.693 | silver.emissoes_co2.co2_uso_terra_mt ← bronze.owid_co2_raw.land_use_change_co2 | 19,2 |
| `co2_incl_uso_terra_mt` | double | Emissões totais de CO2 incluindo mudança no uso da terra (Mt). | -84,56 a 43.184 | silver.emissoes_co2.co2_incl_uso_terra_mt ← bronze.owid_co2_raw.co2_including_luc | 49,4 |
| `co2_acumulado_mt` | double | Emissões acumuladas de CO2, sem uso da terra, desde o primeiro ano com dados (Mt). | 0 a 1.849.124 | silver.emissoes_co2.co2_acumulado_mt ← bronze.owid_co2_raw.cumulative_co2 | 42,1 |
| `co2_acumulado_incl_uso_terra_mt` | double | Emissões acumuladas de CO2 incluindo uso da terra (Mt). | -120,34 a 2.751.504 | silver.emissoes_co2.co2_acumulado_incl_uso_terra_mt ← bronze.owid_co2_raw.cumulative_co2_including_luc | 49,4 |
| `co2_per_capita_t` | double | Emissões de CO2 por pessoa, sem uso da terra (toneladas por pessoa). | 0 a 782,74 | silver.emissoes_co2.co2_per_capita_t ← bronze.owid_co2_raw.co2_per_capita | 44,0 |
| `co2_por_pib_kg_por_usd` | double | Intensidade de carbono da economia: kg de CO2 por dólar internacional de PIB. | 0 a 82,60 | silver.emissoes_co2.co2_por_pib_kg_por_usd ← bronze.owid_co2_raw.co2_per_gdp | 63,4 |
| `co2_consumo_mt` | double | Emissões baseadas no consumo, ajustadas pelo comércio internacional (Mt). | 0 a 38.599 | silver.emissoes_co2.co2_consumo_mt ← bronze.owid_co2_raw.consumption_co2 | 90,0 |
| `co2_comercio_liquido_mt` | double | CO2 líquido embutido no comércio (Mt); negativo indica exportador líquido de emissões. | -2.178 a 1.769 | silver.emissoes_co2.co2_comercio_liquido_mt ← bronze.owid_co2_raw.trade_co2 | 90,0 |
| `energia_primaria_twh` | double | Consumo de energia primária (TWh). | 0 a 176.737 | silver.emissoes_co2.energia_primaria_twh ← bronze.owid_co2_raw.primary_energy_consumption | 76,5 |
| `energia_per_capita_kwh` | double | Consumo de energia primária por pessoa (kWh). | 0 a 318.560 | silver.emissoes_co2.energia_per_capita_kwh ← bronze.owid_co2_raw.energy_per_capita | 76,6 |
| `co2_por_energia_g_kwh` | double | Intensidade de carbono da energia: gramas de CO2 por kWh de energia primária. | 0 a 10.689 | silver.emissoes_co2.co2_por_energia_g_kwh ← bronze.owid_co2_raw.co2_per_unit_energy | 76,7 |
| `participacao_co2_mundial_pct` | double | Participação nas emissões mundiais de CO2 do ano (%). | 0 a 100,00 | silver.emissoes_co2.participacao_co2_mundial_pct ← bronze.owid_co2_raw.share_global_co2 | 42,1 |
| `participacao_co2_acumulado_mundial_pct` | double | Participação nas emissões mundiais acumuladas de CO2 (%). | 0 a 100,00 | silver.emissoes_co2.participacao_co2_acumulado_mundial_pct ← bronze.owid_co2_raw.share_global_cumulative_co2 | 42,1 |
| `gee_total_mt_co2e` | double | Emissões totais de gases de efeito estufa, incluindo uso da terra (Mt de CO2 equivalente, horizonte de 100 anos). | -19,73 a 54.433 | silver.emissoes_co2.gee_total_mt_co2e ← bronze.owid_co2_raw.total_ghg | 18,4 |
| `metano_mt_co2e` | double | Emissões de metano (Mt de CO2 equivalente). | 0 a 9.499 | silver.emissoes_co2.metano_mt_co2e ← bronze.owid_co2_raw.methane | 18,4 |
| `oxido_nitroso_mt_co2e` | double | Emissões de óxido nitroso (Mt de CO2 equivalente). | 0 a 2.936 | silver.emissoes_co2.oxido_nitroso_mt_co2e ← bronze.owid_co2_raw.nitrous_oxide | 17,7 |
| `sk_localidade` | int | Chave da localidade (FK → dim_localidade). | 1 a 255 | dim_localidade.sk_localidade via nome_localidade | 0,0 |
| `ano` | int | Ano (FK → dim_ano). | 1750 a 2024 | silver.emissoes_co2.ano | 0,0 |
| `populacao` | bigint | População total (pessoas). | 215 a 8.161.972.574 | silver.emissoes_co2.populacao ← bronze.owid_co2_raw.population | 9,4 |
| `pib_usd_ppc_2011` | double | PIB em dólares internacionais a preços de 2011 (ajustado por inflação e paridade de poder de compra); disponível até 2022. | 49.980.000 a 130.112.562.171.125 | silver.emissoes_co2.pib_usd_ppc_2011 ← bronze.owid_co2_raw.gdp | 66,3 |
| `co2_mt` | double | Emissões anuais de CO2 de combustíveis fósseis e indústria, sem mudança no uso da terra, em milhões de toneladas (Mt). | 0 a 38.599 | silver.emissoes_co2.co2_mt ← bronze.owid_co2_raw.co2 | 42,1 |
| `co2_carvao_mt` | double | Emissões de CO2 da queima de carvão (Mt). | 0 a 15.805 | silver.emissoes_co2.co2_carvao_mt ← bronze.owid_co2_raw.coal_co2 | 54,1 |
| `co2_petroleo_mt` | double | Emissões de CO2 da queima de petróleo (Mt). | 0 a 12.471 | silver.emissoes_co2.co2_petroleo_mt ← bronze.owid_co2_raw.oil_co2 | 46,3 |
| `co2_gas_mt` | double | Emissões de CO2 da queima de gás natural (Mt). | 0 a 8.010 | silver.emissoes_co2.co2_gas_mt ← bronze.owid_co2_raw.gas_co2 | 62,5 |

### gold.fato_energia_fonte

Fato de energia: uma linha por localidade, ano e fonte, com consumo de energia primária e geração de eletricidade. Colunas da silver transformadas em linhas (stack).

**Linhas:** 78.154

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `sk_localidade` | int | Chave da localidade (FK → dim_localidade). | 1 a 255 | dim_localidade.sk_localidade via nome_localidade | 0,0 |
| `ano` | int | Ano (FK → dim_ano). | 1965 a 2025 | silver.energia.ano | 0,0 |
| `sk_fonte` | int | Chave da fonte de energia (FK → dim_fonte_energia). | 1 a 9 | dim_fonte_energia via codigo_fonte | 0,0 |
| `consumo_primario_twh` | double | Consumo de energia primária da fonte (TWh, método de substituição). | 0 a 55.292 | silver.energia.consumo_<fonte>_twh (stack) | 37,4 |
| `participacao_primaria_pct` | double | Participação da fonte no consumo total de energia primária (%). | 0 a 100,00 | silver.energia.participacao_<fonte>_pct (stack) | 40,8 |
| `geracao_eletrica_twh` | double | Geração de eletricidade da fonte (TWh); nula quando a abertura por fonte não fecha com o total. | 0 a 10.539 | silver.energia.eletricidade_<fonte>_twh (stack + regra de qualidade) | 28,3 |
| `participacao_eletrica_pct` | double | Participação da fonte na geração de eletricidade (%); nula quando a abertura não fecha com o total. | 0 a 100,00 | silver.energia.participacao_eletricidade_<fonte>_pct (stack + regra de qualidade) | 28,3 |

## Camada Silver (dados limpos)

### silver.emissoes_co2

Emissões anuais de CO2 e outros gases por localidade, limpas e tipadas, sem agregados de fontes específicas. Uma linha por localidade e ano.

**Linhas:** 45.267

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `nome_localidade` | string | Nome da localidade como publicado pela OWID (em inglês). Chave natural. | 233 valores distintos (ex.: Afghanistan, Africa, Albania) | bronze.*.country | 0,0 |
| `codigo_localidade` | string | Código ISO 3166-1 alfa-3 do país ou, na falta dele, código da OWID (ex.: OWID_KOS, OWID_USS, OWID_WRL). | 227 valores distintos (ex.: ABW, AFG, AGO) | iso_code; senão regions.yml (busca por nome e nomes alternativos) | 3,6 |
| `tipo_localidade` | string | Classificação da localidade. | {Bloco econômico, Continente, Grupo de renda, Mundo, País, Transporte internacional} | regra de classificação do notebook 04 | 0,0 |
| `ano` | int | Ano de referência. | 1750 a 2024 | bronze.owid_co2_raw.year (texto → inteiro) | 0,0 |
| `populacao` | bigint | População total (pessoas). | 215 a 8.161.972.574 | bronze.owid_co2_raw.population (texto → número) | 9,4 |
| `pib_usd_ppc_2011` | double | PIB em dólares internacionais a preços de 2011 (ajustado por inflação e paridade de poder de compra); disponível até 2022. | 49.980.000 a 130.112.562.171.125 | bronze.owid_co2_raw.gdp (texto → número) | 66,3 |
| `co2_mt` | double | Emissões anuais de CO2 de combustíveis fósseis e indústria, sem mudança no uso da terra, em milhões de toneladas (Mt). | 0 a 38.599 | bronze.owid_co2_raw.co2 (texto → número) | 42,1 |
| `co2_carvao_mt` | double | Emissões de CO2 da queima de carvão (Mt). | 0 a 15.805 | bronze.owid_co2_raw.coal_co2 (texto → número) | 54,1 |
| `co2_petroleo_mt` | double | Emissões de CO2 da queima de petróleo (Mt). | 0 a 12.471 | bronze.owid_co2_raw.oil_co2 (texto → número) | 46,3 |
| `co2_gas_mt` | double | Emissões de CO2 da queima de gás natural (Mt). | 0 a 8.010 | bronze.owid_co2_raw.gas_co2 (texto → número) | 62,5 |
| `co2_cimento_mt` | double | Emissões de CO2 da produção de cimento (Mt). | 0 a 1.667 | bronze.owid_co2_raw.cement_co2 (texto → número) | 38,6 |
| `co2_flaring_mt` | double | Emissões de CO2 da queima de gás em flare na produção de petróleo e gás (Mt). | 0 a 432,39 | bronze.owid_co2_raw.flaring_co2 (texto → número) | 66,8 |
| `co2_outras_industrias_mt` | double | Emissões de CO2 de outros processos industriais (Mt). | 0 a 458,18 | bronze.owid_co2_raw.other_industry_co2 (texto → número) | 93,2 |
| `co2_uso_terra_mt` | double | Emissões líquidas de CO2 por mudança no uso da terra, como desmatamento (Mt); negativo indica sumidouro de carbono. | -319,42 a 8.693 | bronze.owid_co2_raw.land_use_change_co2 (texto → número) | 19,2 |
| `co2_incl_uso_terra_mt` | double | Emissões totais de CO2 incluindo mudança no uso da terra (Mt). | -84,56 a 43.184 | bronze.owid_co2_raw.co2_including_luc (texto → número) | 49,4 |
| `co2_acumulado_mt` | double | Emissões acumuladas de CO2, sem uso da terra, desde o primeiro ano com dados (Mt). | 0 a 1.849.124 | bronze.owid_co2_raw.cumulative_co2 (texto → número) | 42,1 |
| `co2_acumulado_incl_uso_terra_mt` | double | Emissões acumuladas de CO2 incluindo uso da terra (Mt). | -120,34 a 2.751.504 | bronze.owid_co2_raw.cumulative_co2_including_luc (texto → número) | 49,4 |
| `co2_per_capita_t` | double | Emissões de CO2 por pessoa, sem uso da terra (toneladas por pessoa). | 0 a 782,74 | bronze.owid_co2_raw.co2_per_capita (texto → número) | 44,0 |
| `co2_por_pib_kg_por_usd` | double | Intensidade de carbono da economia: kg de CO2 por dólar internacional de PIB. | 0 a 82,60 | bronze.owid_co2_raw.co2_per_gdp (texto → número) | 63,4 |
| `co2_consumo_mt` | double | Emissões baseadas no consumo, ajustadas pelo comércio internacional (Mt). | 0 a 38.599 | bronze.owid_co2_raw.consumption_co2 (texto → número) | 90,0 |
| `co2_comercio_liquido_mt` | double | CO2 líquido embutido no comércio (Mt); negativo indica exportador líquido de emissões. | -2.178 a 1.769 | bronze.owid_co2_raw.trade_co2 (texto → número) | 90,0 |
| `energia_primaria_twh` | double | Consumo de energia primária (TWh). | 0 a 176.737 | bronze.owid_co2_raw.primary_energy_consumption (texto → número) | 76,5 |
| `energia_per_capita_kwh` | double | Consumo de energia primária por pessoa (kWh). | 0 a 318.560 | bronze.owid_co2_raw.energy_per_capita (texto → número) | 76,6 |
| `co2_por_energia_g_kwh` | double | Intensidade de carbono da energia: gramas de CO2 por kWh de energia primária. | 0 a 10.689 | bronze.owid_co2_raw.co2_per_unit_energy (texto → número) | 76,7 |
| `participacao_co2_mundial_pct` | double | Participação nas emissões mundiais de CO2 do ano (%). | 0 a 100,00 | bronze.owid_co2_raw.share_global_co2 (texto → número) | 42,1 |
| `participacao_co2_acumulado_mundial_pct` | double | Participação nas emissões mundiais acumuladas de CO2 (%). | 0 a 100,00 | bronze.owid_co2_raw.share_global_cumulative_co2 (texto → número) | 42,1 |
| `gee_total_mt_co2e` | double | Emissões totais de gases de efeito estufa, incluindo uso da terra (Mt de CO2 equivalente, horizonte de 100 anos). | -19,73 a 54.433 | bronze.owid_co2_raw.total_ghg (texto → número) | 18,4 |
| `metano_mt_co2e` | double | Emissões de metano (Mt de CO2 equivalente). | 0 a 9.499 | bronze.owid_co2_raw.methane (texto → número) | 18,4 |
| `oxido_nitroso_mt_co2e` | double | Emissões de óxido nitroso (Mt de CO2 equivalente). | 0 a 2.936 | bronze.owid_co2_raw.nitrous_oxide (texto → número) | 17,7 |
| `_versao_origem` | string | Commit do repositório da OWID de onde o arquivo foi baixado (versão exata do dado). | {382ee6c662b0ece26e111f263b44c029afad7787} | bronze._versao_origem | 0,0 |
| `_data_processamento` | timestamp | Data e hora em que a linha foi gravada na camada silver. | 2026-09-27 18:46:43.664633 a 2026-09-27 18:46:43.664633 | gerado no notebook 04 | 0,0 |

### silver.energia

Consumo de energia primária e geração de eletricidade por fonte e localidade, limpos e tipados. Uma linha por localidade e ano; 4 colunas por fonte.

**Linhas:** 11.732

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `consumo_outras_renovaveis_twh` | double | Consumo de energia primária de outras renováveis (geotérmica, marés etc., sem bioenergia) (TWh, método de substituição). | 0 a 2.476 | bronze.owid_energia_raw.other_renewable_consumption | 56,1 |
| `participacao_outras_renovaveis_pct` | double | Participação de outras renováveis (geotérmica, marés etc., sem bioenergia) no consumo de energia primária (%). | 0 a 31,24 | bronze.owid_energia_raw.other_renewables_share_energy | 56,2 |
| `eletricidade_outras_renovaveis_twh` | double | Geração de eletricidade a partir de outras renováveis (geotérmica, marés etc., sem bioenergia) (TWh). | 0 a 176,65 | bronze.owid_energia_raw.other_renewable_exc_biofuel_electricity (ou other_renewable_electricity − biofuel_electricity) | 27,0 |
| `participacao_eletricidade_outras_renovaveis_pct` | double | Participação de outras renováveis (geotérmica, marés etc., sem bioenergia) na geração de eletricidade (%). | 0 a 48,45 | bronze.owid_energia_raw.other_renewables_share_elec_exc_biofuel (ou other_renewables_share_elec − biofuel_share_elec) | 40,3 |
| `energia_primaria_twh` | double | Consumo total de energia primária (TWh). | 0 a 176.737 | bronze.owid_energia_raw.primary_energy_consumption | 4,8 |
| `participacao_baixo_carbono_pct` | double | Participação das fontes de baixo carbono (renováveis + nuclear) na energia primária (%). | 0 a 86,13 | bronze.owid_energia_raw.low_carbon_share_energy | 56,2 |
| `participacao_renovaveis_pct` | double | Participação das renováveis na energia primária (%). | 0 a 86,13 | bronze.owid_energia_raw.renewables_share_energy | 56,2 |
| `participacao_fosseis_pct` | double | Participação dos combustíveis fósseis na energia primária (%). | 13,87 a 100,00 | bronze.owid_energia_raw.fossil_share_energy | 56,2 |
| `geracao_eletrica_twh` | double | Geração total de eletricidade (TWh). | 0 a 31.772 | bronze.owid_energia_raw.electricity_generation | 39,7 |
| `participacao_eletricidade_baixo_carbono_pct` | double | Participação das fontes de baixo carbono na geração de eletricidade (%). | 0 a 100,00 | bronze.owid_energia_raw.low_carbon_share_elec | 40,1 |
| `intensidade_carbono_eletricidade_g_kwh` | double | Gases de efeito estufa emitidos por kWh de eletricidade gerada (g CO2e/kWh). | 0 a 1.307 | bronze.owid_energia_raw.carbon_intensity_elec | 48,7 |
| `eletricidade_abertura_completa` | boolean | Verdadeiro se a soma da geração por fonte fecha com a geração total (± 1%). | {false, true} | calculado no notebook 04 | 0,0 |
| `_versao_origem` | string | Commit do repositório da OWID de onde o arquivo foi baixado (versão exata do dado). | {7e387a16f70a510e433f8aac7efeac6faa1e5059} | bronze._versao_origem | 0,0 |
| `_data_processamento` | timestamp | Data e hora em que a linha foi gravada na camada silver. | 2026-09-27 18:46:51.068852 a 2026-09-27 18:46:51.068852 | gerado no notebook 04 | 0,0 |
| `nome_localidade` | string | Nome da localidade como publicado pela OWID (em inglês). Chave natural. | 239 valores distintos (ex.: Afghanistan, Africa, Albania) | bronze.*.country | 0,0 |
| `codigo_localidade` | string | Código ISO 3166-1 alfa-3 do país ou, na falta dele, código da OWID (ex.: OWID_KOS, OWID_USS, OWID_WRL). | 235 valores distintos (ex.: ABW, AFG, AGO) | iso_code; senão regions.yml (busca por nome e nomes alternativos) | 2,0 |
| `tipo_localidade` | string | Classificação da localidade. | {Bloco econômico, Continente, Grupo de renda, Mundo, País, País histórico} | regra de classificação do notebook 04 | 0,0 |
| `ano` | int | Ano de referência. | 1965 a 2025 | bronze.owid_energia_raw.year (texto → inteiro) | 0,0 |
| `consumo_carvao_twh` | double | Consumo de energia primária de carvão (TWh, método de substituição). | 0 a 45.851 | bronze.owid_energia_raw.coal_consumption | 56,1 |
| `participacao_carvao_pct` | double | Participação de carvão no consumo de energia primária (%). | 0 a 89,16 | bronze.owid_energia_raw.coal_share_energy | 56,2 |
| `eletricidade_carvao_twh` | double | Geração de eletricidade a partir de carvão (TWh). | 0 a 10.539 | bronze.owid_energia_raw.coal_electricity | 44,7 |
| `participacao_eletricidade_carvao_pct` | double | Participação de carvão na geração de eletricidade (%). | 0 a 100,00 | bronze.owid_energia_raw.coal_share_elec | 44,9 |
| `consumo_petroleo_twh` | double | Consumo de energia primária de petróleo (TWh, método de substituição). | 0 a 55.292 | bronze.owid_energia_raw.oil_consumption | 56,1 |
| `participacao_petroleo_pct` | double | Participação de petróleo no consumo de energia primária (%). | 8,06 a 100,00 | bronze.owid_energia_raw.oil_share_energy | 56,2 |
| `eletricidade_petroleo_twh` | double | Geração de eletricidade a partir de petróleo (TWh). | 0 a 1.366 | bronze.owid_energia_raw.oil_electricity | 44,9 |
| `participacao_eletricidade_petroleo_pct` | double | Participação de petróleo na geração de eletricidade (%). | 0 a 100,00 | bronze.owid_energia_raw.oil_share_elec | 45,1 |
| `consumo_gas_natural_twh` | double | Consumo de energia primária de gás natural (TWh, método de substituição). | 0 a 41.278 | bronze.owid_energia_raw.gas_consumption | 56,1 |
| `participacao_gas_natural_pct` | double | Participação de gás natural no consumo de energia primária (%). | 0 a 91,94 | bronze.owid_energia_raw.gas_share_energy | 56,2 |
| `eletricidade_gas_natural_twh` | double | Geração de eletricidade a partir de gás natural (TWh). | 0 a 6.921 | bronze.owid_energia_raw.gas_electricity | 45,9 |
| `participacao_eletricidade_gas_natural_pct` | double | Participação de gás natural na geração de eletricidade (%). | 0 a 100,00 | bronze.owid_energia_raw.gas_share_elec | 46,2 |
| `consumo_nuclear_twh` | double | Consumo de energia primária de energia nuclear (TWh, método de substituição). | 0 a 7.495 | bronze.owid_energia_raw.nuclear_consumption | 43,8 |
| `participacao_nuclear_pct` | double | Participação de energia nuclear no consumo de energia primária (%). | 0 a 41,66 | bronze.owid_energia_raw.nuclear_share_energy | 56,2 |
| `eletricidade_nuclear_twh` | double | Geração de eletricidade a partir de energia nuclear (TWh). | 0 a 2.812 | bronze.owid_energia_raw.nuclear_electricity | 22,5 |
| `participacao_eletricidade_nuclear_pct` | double | Participação de energia nuclear na geração de eletricidade (%). | 0 a 88,01 | bronze.owid_energia_raw.nuclear_share_elec | 41,4 |
| `consumo_hidreletrica_twh` | double | Consumo de energia primária de hidrelétrica (TWh, método de substituição). | 0 a 10.861 | bronze.owid_energia_raw.hydro_consumption | 56,1 |
| `participacao_hidreletrica_pct` | double | Participação de hidrelétrica no consumo de energia primária (%). | 0 a 71,42 | bronze.owid_energia_raw.hydro_share_energy | 56,2 |
| `eletricidade_hidreletrica_twh` | double | Geração de eletricidade a partir de hidrelétrica (TWh). | 0 a 4.435 | bronze.owid_energia_raw.hydro_electricity | 27,5 |
| `participacao_eletricidade_hidreletrica_pct` | double | Participação de hidrelétrica na geração de eletricidade (%). | 0 a 100,00 | bronze.owid_energia_raw.hydro_share_elec | 40,9 |
| `consumo_solar_twh` | double | Consumo de energia primária de solar (TWh, método de substituição). | 0 a 5.151 | bronze.owid_energia_raw.solar_consumption | 56,1 |
| `participacao_solar_pct` | double | Participação de solar no consumo de energia primária (%). | 0 a 9,88 | bronze.owid_energia_raw.solar_share_energy | 56,2 |
| `eletricidade_solar_twh` | double | Geração de eletricidade a partir de solar (TWh). | 0 a 2.779 | bronze.owid_energia_raw.solar_electricity | 26,6 |
| `participacao_eletricidade_solar_pct` | double | Participação de solar na geração de eletricidade (%). | 0 a 50,00 | bronze.owid_energia_raw.solar_share_elec | 40,1 |
| `consumo_eolica_twh` | double | Consumo de energia primária de eólica (TWh, método de substituição). | 0 a 6.124 | bronze.owid_energia_raw.wind_consumption | 56,1 |
| `participacao_eolica_pct` | double | Participação de eólica no consumo de energia primária (%). | 0 a 26,09 | bronze.owid_energia_raw.wind_share_energy | 56,2 |
| `eletricidade_eolica_twh` | double | Geração de eletricidade a partir de eólica (TWh). | 0 a 2.713 | bronze.owid_energia_raw.wind_electricity | 28,4 |
| `participacao_eletricidade_eolica_pct` | double | Participação de eólica na geração de eletricidade (%). | 0 a 58,21 | bronze.owid_energia_raw.wind_share_elec | 41,9 |
| `consumo_biocombustiveis_twh` | double | Consumo de energia primária de biocombustíveis e bioenergia (TWh, método de substituição). | 0 a 1.367 | bronze.owid_energia_raw.biofuel_consumption | 46,0 |
| `participacao_biocombustiveis_pct` | double | Participação de biocombustíveis e bioenergia no consumo de energia primária (%). | 0 a 7,58 | bronze.owid_energia_raw.biofuel_share_energy | 56,2 |
| `eletricidade_biocombustiveis_twh` | double | Geração de eletricidade a partir de biocombustíveis e bioenergia (TWh). | 0 a 710,41 | bronze.owid_energia_raw.biofuel_electricity | 48,7 |
| `participacao_eletricidade_biocombustiveis_pct` | double | Participação de biocombustíveis e bioenergia na geração de eletricidade (%). | 0 a 77,59 | bronze.owid_energia_raw.biofuel_share_elec | 49,0 |

### silver.localidades

Todas as localidades das duas bases, com código, tipo, continente e a marcação dos agregados descartados. Uma linha por localidade.

**Linhas:** 349

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `nome_localidade` | string | Nome da localidade como publicado pela OWID (em inglês). Chave natural. | 349 valores distintos (ex.: ASEAN (Ember), Afghanistan, Africa) | bronze.*.country | 0,0 |
| `codigo_localidade` | string | Código ISO 3166-1 alfa-3 do país ou, na falta dele, código da OWID (ex.: OWID_KOS, OWID_USS, OWID_WRL). | 261 valores distintos (ex.: ABW, AFG, AGO) | iso_code; senão regions.yml (busca por nome e nomes alternativos) | 25,2 |
| `codigo_iso` | string | Código ISO 3166-1 alfa-3 como veio da origem (nulo para agregados e países históricos). | 232 valores distintos (ex.: ABW, AFG, AGO) | bronze.*.iso_code | 33,5 |
| `tipo_localidade` | string | Classificação da localidade. | {Agregado de fonte específica, Bloco econômico, Continente, Grupo de renda, Mundo, Outro, País, País histórico, Transporte internacional} | regra de classificação do notebook 04 | 0,0 |
| `is_pais` | boolean | Verdadeiro para países atuais: use para rankings e somas sem dupla contagem. | {false, true} | tipo_localidade = 'País' | 0,0 |
| `is_historico` | boolean | Verdadeiro para países que não existem mais. | {false, true} | silver.regioes_owid.is_historico | 0,0 |
| `ano_fim_existencia` | int | Último ano de existência de um país histórico. | 1990 a 2010 | silver.regioes_owid.ano_fim_existencia | 98,0 |
| `continente` | string | Continente em português (Antártida atribuída manualmente). | {América do Norte, América do Sul, Antártida, Europa, Oceania, África, Ásia} | silver.regioes_owid.continente | 29,8 |
| `presente_em_co2` | boolean | Verdadeiro se a localidade aparece no arquivo de CO2. | {false, true} | bronze.owid_co2_raw | 0,0 |
| `presente_em_energia` | boolean | Verdadeiro se a localidade aparece no arquivo de energia. | {false, true} | bronze.owid_energia_raw | 0,0 |
| `descartada` | boolean | Verdadeiro para agregados de fontes específicas, que não seguem para as tabelas de fatos. | {false, true} | tipo_localidade = 'Agregado de fonte específica' | 0,0 |
| `motivo_descarte` | string | Motivo do descarte. | {agregado regional/econômico definido por uma fonte específica; redundante com os agregados da OWID} | regra de classificação do notebook 04 | 73,1 |
| `_data_processamento` | timestamp | Data e hora em que a linha foi gravada na camada silver. | 2026-09-27 18:46:35.054189 a 2026-09-27 18:46:35.054189 | gerado no notebook 04 | 0,0 |

### silver.regioes_owid

Referência oficial de regiões da OWID (regions.yml): códigos, países históricos e continente de cada membro. Uma linha por região.

**Linhas:** 476

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `codigo_regiao` | string | Código da região (ISO alfa-3 para países ou código OWID_*). Chave. | 476 valores distintos (ex.: ABW, AFG, AGO) | bronze.owid_regioes_raw.code | 0,0 |
| `nome_regiao` | string | Nome da região. | 476 valores distintos (ex.: Abkhazia, Afghanistan, Africa) | bronze.owid_regioes_raw.name | 0,0 |
| `tipo_regiao_owid` | string | Tipo da região segundo a OWID. | {aggregate, continent, country, other} | bronze.owid_regioes_raw.region_type (nulo = country) | 0,0 |
| `is_historico` | boolean | Verdadeiro se a região não existe mais (ex.: URSS). | {false, true} | bronze.owid_regioes_raw.is_historical | 0,0 |
| `ano_fim_existencia` | int | Último ano de existência de uma região histórica. | 1830 a 2011 | bronze.owid_regioes_raw.end_year | 94,1 |
| `nomes_alternativos` | array<string> | Nomes alternativos usados para encontrar a região pelo nome. | lista de textos | bronze.owid_regioes_raw.aliases | 0,0 |
| `continente` | string | Continente (em português) ao qual a região pertence. | {América do Norte, América do Sul, Europa, Oceania, África, Ásia} | bronze.owid_regioes_raw.members dos 6 continentes (explode) | 39,5 |
| `_data_processamento` | timestamp | Data e hora em que a linha foi gravada na camada silver. | 2026-09-27 18:46:31.200625 a 2026-09-27 18:46:31.200625 | gerado no notebook 04 | 0,0 |

## Governança (metadados do pipeline)

### governanca.controle_ingestao

Log de auditoria da ingestão (append): uma linha por arquivo por execução do notebook 02.

**Linhas:** 5

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `data_ingestao` | timestamp | Data e hora da execução da ingestão (UTC). | 2026-09-27 18:45:33 a 2026-09-27 18:45:33 | notebook 02 | 0,0 |
| `arquivo` | string | Nome do arquivo de origem. | {owid-co2-codebook.csv, owid-co2-data.csv, owid-energy-codebook.csv, owid-energy-data.csv, regions.yml} | configuração (00) | 0,0 |
| `url_origem` | string | URL oficial (fixada em um commit) de onde o arquivo foi baixado. | {https://raw.githubusercontent.com/owid/co2-data/382ee6c662b0ece26e111f263b44c029afad7787/owid-co2-codebook.csv, https://raw.githubusercontent.com/owid/co2-data/382ee6c662b0ece26e111f263b44c029afad7787/owid-co2-data.csv, https://raw.githubusercontent.com/owid/energy-data/7e387a16f70a510e433f8aac7efeac6faa1e5059/owid-energy-codebook.csv, https://raw.githubusercontent.com/owid/energy-data/7e387a16f70a510e433f8aac7efeac6faa1e5059/owid-energy-data.csv, https://raw.githubusercontent.com/owid/etl/32cf87e08ca01077e603c6383b81279eda7e8400/etl/steps/data/garden/regions/2023-01-01/regions.yml} | configuração (00) | 0,0 |
| `versao_origem` | string | Commit do repositório da OWID. | {32cf87e08ca01077e603c6383b81279eda7e8400, 382ee6c662b0ece26e111f263b44c029afad7787, 7e387a16f70a510e433f8aac7efeac6faa1e5059} | configuração (00) | 0,0 |
| `sha256` | string | Hash SHA-256 calculado sobre o arquivo recebido. | {266f2e2baad7975351bc9bb4aa061d22b1da9fe4c47d51d2ac6071e01e171f76, 33b4f5e00efd58c7b83863f736beba1af4df946b43642b0400c3ec38648e0e8e, 3cc9b7db0d921496e2988568ce3aee5ed41f50431dd234a0663b5f0a4b2e32bb, 4a38164e30245da2ca800792c62d2ca02e61c824fe021fb6ef3777b4c3ac9003, 7f78e2b218ce4bb8c538bbec04fdc9a7982e8d40bff972e650df603899edd5f6} | notebook 02 | 0,0 |
| `sha256_confere` | boolean | Verdadeiro se o hash confere com o da versão fixada. | {true} | notebook 02 | 0,0 |
| `tamanho_bytes` | bigint | Tamanho do arquivo em bytes. | 24.149 a 14.377.942 | notebook 02 | 0,0 |
| `modo_coleta` | string | Como o arquivo chegou ao Volume (download automático ou reaproveitado/upload manual). | {reaproveitado do Volume (upload manual ou execução anterior)} | notebook 02 | 0,0 |
| `tabela_destino` | string | Tabela bronze gerada a partir do arquivo. | {spark_catalog.bronze.owid_co2_codebook_raw, spark_catalog.bronze.owid_co2_raw, spark_catalog.bronze.owid_energia_codebook_raw, spark_catalog.bronze.owid_energia_raw, spark_catalog.bronze.owid_regioes_raw} | notebook 02 | 0,0 |
| `linhas_carregadas` | bigint | Quantidade de linhas gravadas na tabela bronze. | 79 a 50.411 | notebook 02 | 0,0 |

### governanca.resultados_qualidade

Resultados das verificações de qualidade (append): diagnóstico da bronze (03) e testes pós-carga da silver (04) e da gold (05).

**Linhas:** 132

| Coluna | Tipo | Descrição | Domínio | Origem (linhagem) | % nulos |
|---|---|---|---|---|---:|
| `data_execucao` | timestamp | Data e hora da execução (UTC). | 2026-09-27 18:46:29 a 2026-09-27 18:47:27 | notebooks 03, 04 e 05 | 0,0 |
| `etapa` | string | Notebook/camada que executou a verificação. | {03 diagnóstico (bronze), 04 silver, 05 gold} | notebooks 03, 04 e 05 | 0,0 |
| `dimensao` | string | Dimensão de qualidade: Completude, Consistência, Unicidade, Acurácia, Outliers ou Integridade. | {Acurácia, Completude, Consistência, Integridade, Outliers, Unicidade} | notebooks 03, 04 e 05 | 0,0 |
| `tabela` | string | Tabela verificada. | {co2 x energia, gold.dim_ano, gold.dim_fonte_energia, gold.dim_localidade, gold.fato_emissoes_anual, gold.fato_energia_fonte, owid_co2_raw, owid_energia_raw, owid_regioes_raw, silver.emissoes_co2, silver.energia, silver.localidades} | notebooks 03, 04 e 05 | 0,0 |
| `verificacao` | string | Descrição da verificação. | 121 valores distintos (ex.: % de nulos em co2, % de nulos em co2_per_capita, % de nulos em co2_per_unit_energy) | notebooks 03, 04 e 05 | 0,0 |
| `valor_encontrado` | double | Valor medido (contagem, percentual ou desvio). | 0 a 5.708 | notebooks 03, 04 e 05 | 0,0 |
| `esperado` | string | Valor esperado. | {0, 1,0, < 0,1 p.p., < 0,5%, informativo, ≈ 0, ≈ 1,0} | notebooks 03, 04 e 05 | 0,0 |
| `status` | string | OK, ALERTA (problema conhecido e tratado) ou FALHA. | {ALERTA, FALHA, OK} | notebooks 03, 04 e 05 | 0,0 |
| `observacao` | string | Contexto do resultado e tratamento aplicado. | {, a matriz elétrica por fonte só é usada a partir de 2000, a soma das fontes não fecha o total: participações devem vir das colunas *_share_energy, calculadas sobre o total, agregados (Mundo, continentes, grupos de renda...) e países históricos, ver seção 6, agregados misturados com países: exige classificar as localidades na silver, cobertura depende do ano e da localidade (ver seção 8), confirma que, separados os agregados, as partes somam o todo, sumidouro de carbono / exportador líquido de emissões, valores reais (petroestados e microterritórios): mantidos, mas rankings per capita usam população mínima} | notebooks 03, 04 e 05 | 0,0 |
