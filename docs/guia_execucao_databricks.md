# Guia de execução no Databricks Free Edition

A execução completa do pipeline leva cerca de 12 minutos. A execução registrada em 27/09/2026 levou 11,5 minutos.

---

## 1. Preparar o ambiente

1. Crie ou acesse uma conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition). O cadastro é gratuito.
2. No menu lateral, abra **Workspace**, entre na pasta do usuário e use **Create → Git folder**.
   - **Git repository URL:** `https://github.com/Gianluca-Scofano/mvp-engenharia-dados-co2-energia.git`
   - **Git provider:** GitHub
   - Clique em **Create Git folder**. O repositório é público, então não é preciso configurar credenciais para clonar.
3. Abra a pasta `notebooks` dentro da Git folder.

**Se a opção "Git folder" não aparecer**, importe os notebooks:

1. Baixe o ZIP do repositório no GitHub (**Code → Download ZIP**) e extraia a pasta `notebooks`.
2. No Databricks, clique com o botão direito em **Home** → **Import** → aba **File**, arraste o ZIP e clique em **Import**.
3. Os notebooks precisam ficar na mesma pasta, porque cada um executa `%run ./00_configuracao`.

Também é possível importar notebook por notebook pela aba **URL**, usando o endereço *raw* de cada arquivo, por exemplo
`https://raw.githubusercontent.com/Gianluca-Scofano/mvp-engenharia-dados-co2-energia/main/notebooks/00_configuracao.py`.

---

## 2. Executar o pipeline

1. Abra o notebook **`99_executar_pipeline_completo`**.
2. Conecte na computação **Serverless** (botão *Connect*, no canto superior direito) e clique em **Run all**.
3. O notebook executa os notebooks `01` a `07` em sequência e, no final, mostra uma tabela com as 7 etapas, todas com status **OK**.

Abaixo da primeira célula aparece um link **"Notebook job #..."** para cada etapa. Ele abre a execução da etapa com todas as saídas
(tabelas, gráficos e testes). Também é possível abrir cada notebook, de `01` a `07`, e usar **Run all** em cada um, na ordem.

**Problemas comuns:**

- **Etapa 01 falha ao criar o catálogo:** em `00_configuracao`, troque `CATALOGO = "mvp_co2_energia"` por `CATALOGO = "workspace"`
  e execute o `99` de novo.
- **Etapa 02 falha no download** (ambiente sem acesso à internet):
  1. Baixe os 5 arquivos pelos links abaixo:
     - [owid-co2-data.csv](https://raw.githubusercontent.com/owid/co2-data/382ee6c662b0ece26e111f263b44c029afad7787/owid-co2-data.csv)
     - [owid-energy-data.csv](https://raw.githubusercontent.com/owid/energy-data/7e387a16f70a510e433f8aac7efeac6faa1e5059/owid-energy-data.csv)
     - [regions.yml](https://raw.githubusercontent.com/owid/etl/32cf87e08ca01077e603c6383b81279eda7e8400/etl/steps/data/garden/regions/2023-01-01/regions.yml)
     - [owid-co2-codebook.csv](https://raw.githubusercontent.com/owid/co2-data/382ee6c662b0ece26e111f263b44c029afad7787/owid-co2-codebook.csv)
     - [owid-energy-codebook.csv](https://raw.githubusercontent.com/owid/energy-data/7e387a16f70a510e433f8aac7efeac6faa1e5059/owid-energy-codebook.csv)
  2. No Databricks, abra **Catalog → mvp_co2_energia → bronze → Volumes → arquivos_brutos → Upload to this volume** e envie os 5
     arquivos com os nomes exatos acima.
  3. Execute o `99` de novo. O `02` reaproveita os arquivos do Volume e confere o hash.

---

## 3. Evidências da execução

As evidências vêm de duas fontes:

1. **Print da tela do notebook `99`**, com as 7 etapas concluídas e a duração de cada uma.
2. **Exportações HTML das etapas.** A página de cada etapa (link **"Notebook job #..."**) foi exportada em HTML. Os arquivos estão
   em [`docs/evidencias/`](evidencias/) e abrem em qualquer navegador (baixe o `.html` e abra localmente). As imagens de
   `docs/img` mostram as saídas das células extraídas dessas exportações, sem alteração nos valores.

A única edição foi ocultar o e-mail da conta, substituído por "(e-mail oculto)" nos dois lugares onde aparecia: no caminho dos
notebooks, no print do `99`, e no campo *Owner* da tabela, na exportação da etapa 06.

| Imagem | Origem | O que mostra |
|---|---|---|
| `11_pipeline_completo.png` | Print do notebook `99` | As 7 etapas com status *Succeeded*/OK e a duração de cada uma |
| `02_volume_arquivos.png` | Etapa 02, seção **1. Coleta dos arquivos para o Volume** | Os 5 arquivos baixados para o Volume, com SHA-256 conferido |
| `03_controle_ingestao.png` | Etapa 02, seção **4. Log de ingestão** | `governanca.controle_ingestao` com `sha256_confere = true` e `linhas_carregadas` |
| `04_qualidade_diagnostico.png` | Etapa 03, seção **6. Localidades** | Soma ingênua de todas as linhas com razão 6,36 sobre o Mundo |
| `05_silver_log_transformacoes.png` | Etapa 04, seções **5** e **6** | Log de transformações (linhas antes/depois) e os 12 testes OK |
| `06_gold_testes.png` | Etapa 05, seção **4. Testes** | Os 14 testes da gold com status OK |
| `07_modelo_er.png` | Etapa 05, seção **3. Restrições** | As 22 restrições (PK, FK, NOT NULL e CHECK) aplicadas |
| `01_catalogo_schemas.png` | Etapa 06, seção **3. Aplicação dos comentários** | 11 tabelas documentadas, 177 colunas e nenhuma sem documentação |
| `08_catalogo_comentarios.png` | Etapa 06, seção **4. Evidência** | `DESCRIBE TABLE EXTENDED` com os comentários lidos do Unity Catalog |
| `p1_...` a `p7_...` | Etapa 07, gráfico de cada pergunta (P1 a P7) | Os 7 gráficos da análise, como o Databricks os gerou |
