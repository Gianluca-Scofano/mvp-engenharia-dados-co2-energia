# Guia de execução no Databricks Free Edition e checklist de evidências

Tempo estimado: **30 a 45 minutos**, sendo 15 minutos de execução do pipeline e o restante para tirar e subir os prints.

---

## Parte 1 · Preparar o ambiente (5 min)

1. Crie ou acesse sua conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition). O cadastro é gratuito e não
   pede cartão.
2. No menu lateral, abra **Workspace**, entre na sua pasta de usuário e use **Create → Git folder**.
   - **Git repository URL:** `https://github.com/Gianluca-Scofano/mvp-engenharia-dados-co2-energia.git`
   - **Git provider:** GitHub
   - Clique em **Create Git folder**. O repositório é público, então não é preciso configurar credenciais para clonar.
3. Abra a pasta `notebooks` dentro da Git folder.

---

## Parte 2 · Executar o pipeline (10–15 min)

Abra cada notebook, conecte na computação **Serverless** (botão *Connect*, no canto superior direito) e clique em **Run all**.
**Respeite a ordem:**

| # | Notebook | Resultado esperado |
|---|---|---|
| 1 | `01_setup_ambiente` | Lista dos schemas `bronze`, `governanca`, `gold` e `silver`, e o Volume `arquivos_brutos` |
| 2 | `02_coleta_bronze` | 5 arquivos baixados com "SHA-256 confere com a versão fixada"; 5 tabelas bronze; log de ingestão |
| 3 | `03_qualidade_diagnostico` | Tabelas e gráficos do diagnóstico; resumo de problemas no final |
| 4 | `04_transformacao_silver` | Log de transformações e 12 testes com status **OK** |
| 5 | `05_modelagem_gold` | 5 tabelas gold, restrições aplicadas e 14 testes com status **OK** |
| 6 | `06_catalogo_dados` | "Colunas documentadas: 177 \| sem documentação: 0" |
| 7 | `07_analise` | Resultados e gráficos das perguntas P1 a P7 |

**Se algo der errado:**

- **Erro ao criar o catálogo no notebook 01:** edite `00_configuracao`, troque `CATALOGO = "mvp_co2_energia"` por
  `CATALOGO = "workspace"` e rode o 01 de novo.
- **Erro de download no notebook 02** (o ambiente pode não ter acesso à internet):
  1. Baixe os 5 arquivos no seu computador pelos links abaixo (botão direito → *Salvar link como*):
     - [owid-co2-data.csv](https://raw.githubusercontent.com/owid/co2-data/382ee6c662b0ece26e111f263b44c029afad7787/owid-co2-data.csv)
     - [owid-energy-data.csv](https://raw.githubusercontent.com/owid/energy-data/7e387a16f70a510e433f8aac7efeac6faa1e5059/owid-energy-data.csv)
     - [regions.yml](https://raw.githubusercontent.com/owid/etl/32cf87e08ca01077e603c6383b81279eda7e8400/etl/steps/data/garden/regions/2023-01-01/regions.yml)
     - [owid-co2-codebook.csv](https://raw.githubusercontent.com/owid/co2-data/382ee6c662b0ece26e111f263b44c029afad7787/owid-co2-codebook.csv)
     - [owid-energy-codebook.csv](https://raw.githubusercontent.com/owid/energy-data/7e387a16f70a510e433f8aac7efeac6faa1e5059/owid-energy-codebook.csv)
  2. No Databricks: **Catalog → mvp_co2_energia → bronze → Volumes → arquivos_brutos → Upload to this volume** e envie os 5 arquivos
     com os nomes exatos acima.
  3. Rode o `02_coleta_bronze` de novo. Ele reaproveita os arquivos do Volume e confere o hash.

---

## Parte 3 · Job do pipeline (opcional, 5 min, vale o print)

1. **Jobs & Pipelines → Create → Job**, com o nome `pipeline_co2_energia`.
2. Crie uma tarefa por notebook, todas do tipo *Notebook* e com computação *Serverless*. Em cada uma, aponte o *Path* para o notebook
   dentro da Git folder e configure **Depends on** para a tarefa anterior:

   | Tarefa | Notebook | Depends on |
   |---|---|---|
   | `coleta_bronze` | `02_coleta_bronze` | — |
   | `qualidade` | `03_qualidade_diagnostico` | `coleta_bronze` |
   | `silver` | `04_transformacao_silver` | `qualidade` |
   | `gold` | `05_modelagem_gold` | `silver` |
   | `catalogo` | `06_catalogo_dados` | `gold` |
   | `analise` | `07_analise` | `catalogo` |

3. Clique em **Run now** e, quando terminar, abra a execução e tire o print do grafo com todas as tarefas verdes.

Se decidir não fazer o Job, apague do `README.md` a imagem `11_job_pipeline.png` e a legenda logo abaixo dela.

---

## Parte 4 · Prints (salve com **exatamente** estes nomes)

As imagens hoje no repositório são *placeholders* ("PRINT PENDENTE"). Os seus prints com o mesmo nome as substituem.

| Arquivo | Onde tirar | O que precisa aparecer |
|---|---|---|
| `01_catalogo_schemas.png` | Menu **Catalog** → expandir `mvp_co2_energia` | Os schemas `bronze`, `silver`, `gold` e `governanca` |
| `02_volume_arquivos.png` | Catalog → `mvp_co2_energia` → `bronze` → Volumes → `arquivos_brutos` | Os 5 arquivos no Volume |
| `03_controle_ingestao.png` | Notebook 02, seção **4. Log de ingestão** | Tabela com `sha256_confere = true` e `linhas_carregadas` |
| `04_qualidade_diagnostico.png` | Notebook 03, seção **6** | Tabela "Soma ingênua de todas as linhas" com razão 6,36 |
| `05_silver_log_transformacoes.png` | Notebook 04, seções **5** e **6** | Log de transformações (linhas antes/depois) e testes OK |
| `06_gold_testes.png` | Notebook 05, seção **4** | Os 14 testes com status OK (se couber, inclua a tabela de restrições da seção 3) |
| `07_modelo_er.png` | Catalog → `gold` → `fato_energia_fonte` → **View relationships** / *Entity relationship diagram* (aba *Overview*) | Diagrama com as chaves PK/FK ligando fatos e dimensões* |
| `08_catalogo_comentarios.png` | Catalog → `gold` → `fato_emissoes_anual` → aba **Overview** | Colunas com os comentários (descrição, domínio, origem) |
| `09_linhagem.png` | Mesma tabela → aba **Lineage** → *See lineage graph* | Grafo bronze → silver → gold |
| `11_job_pipeline.png` | Execução do Job (Parte 3) | Grafo das tarefas, todas com sucesso |
| `p1_maiores_emissores.png` | Notebook 07, gráfico da P1 | Gráfico de barras dos 10 maiores emissores |
| `p2_per_capita.png` | Notebook 07, gráfico da P2 | Barras per capita com o Brasil em laranja |
| `p3_desacoplamento.png` | Notebook 07, gráfico da P3 | Dispersão PIB × CO₂ |
| `p4_transicao_energetica.png` | Notebook 07, gráfico da P4 | Linhas Brasil × Mundo |
| `p5_baixo_carbono_intensidade.png` | Notebook 07, gráfico da P5 | Dispersão com r = −0,67 no título |
| `p6_uso_da_terra_brasil.png` | Notebook 07, gráfico da P6 | Linhas de uso da terra × fóssil |
| `p7_continentes.png` | Notebook 07, gráfico da P7 | Barras 100% por continente |

\* Se o botão do diagrama não aparecer na sua conta, tire o print da aba **Overview** de `fato_energia_fonte` mostrando a seção de
*constraints* (primary key e foreign keys). Outra opção é rodar `DESCRIBE TABLE EXTENDED mvp_co2_energia.gold.fato_energia_fonte`
em uma célula e capturar a parte de *Constraints*.

**Dicas para os prints:**

- Nos prints de notebook, deixe aparecer o **título da seção** e a **barra do Databricks**. Isso comprova que a execução foi na
  plataforma de nuvem.
- Nos gráficos da análise, se couber, inclua também a tabela de resultado logo acima.

---

## Parte 5 · Subir os prints no GitHub (5 min)

1. No GitHub, abra o repositório → pasta `docs/img` → **Add file → Upload files**.
2. Arraste todos os PNGs, com os mesmos nomes da tabela acima, para substituir os *placeholders*.
3. Clique em **Commit changes**.
4. Abra o `README.md` no GitHub e confira se todas as imagens aparecem.

---

## Parte 6 · Revisão final e entrega

- [ ] Todos os notebooks rodaram no Databricks sem erro.
- [ ] Os 17 prints foram substituídos (nenhum "PRINT PENDENTE" no README).
- [ ] A seção **7. Autoavaliação** do README foi revisada com as suas palavras.
- [ ] O repositório está **público**.
- [ ] O link do repositório foi postado no fórum de entrega do MVP.
