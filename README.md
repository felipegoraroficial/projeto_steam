# Extraíndo Dados da API da Steam para Geração de Insights

## Objetivo:

Este projeto usa a API da Steam para buscar informações sobre os jogos que mais joguei na minha biblioteca. Além do tempo de jogo, também analiso gênero, preço e requisitos mínimos para rodar.

Com isso, consigo descobrir quais tipos de jogos não valem a compra e quais funcionam bem no meu PC.

<div align="center">
  <img src="https://github.com/user-attachments/assets/9183f220-9401-4014-9b86-02fb0cec263a" alt="Desenho Funcional">
  <p><b>Desenho Funcional</b></p>
</div>

<p align="left">
</p>

## Introdução Técnica:

Este projeto tem como objetivo realizar a extração e o processamento de dados provenientes da API da Steam, especificamente relacionados ao tempo de jogo (playtime) e aos detalhes dos títulos vinculados à minha conta.

A arquitetura implementada utiliza Azure Functions para consumir a API e armazenar os dados coletados em formato JSON no diretório inbound de uma Azure Storage Account.

A partir desses arquivos inbound, um fluxo de Data Flow no Azure Data Factory (ADF) é responsável por processar apenas dados incrementais, transferindo-os para o diretório bronze da mesma Storage Account. Nesta etapa, o ADF também executa um notebook no Databricks, utilizando um job cluster, para criação de uma tabela externa baseada nos dados armazenados.

Em seguida, o ADF orquestra a execução de um job no Databricks referente a um pipeline Delta Live Tables (DLT), que realiza o processamento das camadas silver e gold, incluindo agregações e métricas de dados. A execução deste job é feita por meio de um Service Principal Name (SPN) com scope configurado no Databricks.

Todo o fluxo — desde a chamada inicial à API via Azure Function até a execução final do pipeline DLT — é integralmente orquestrado pelo Azure Data Factory.

A infraestrutura em nuvem foi provisionada e gerenciada utilizando Terraform, garantindo reprodutibilidade, escalabilidade e versionamento da configuração.

<div align="center">
  <img src="https://github.com/user-attachments/assets/7181cfd1-30af-484a-a32c-55e96d240d63" alt="Desenho Técnico">
  <p><b>Desenho Técnico</b></p>
</div>

<p align="left">
</p>

## Meta:

1. **Captação de Dados Brutos**:
    - **Objetivo**: Criar uma aplicação no Azure Functions para capturar dados brutos com periodicidade diária.
    - **Benefício**: Permite a captura de dados em tempo hábil para a geração de insights.

2. **Processamento e Escalabilidade para BigData**:
    - **Objetivo**: Aplicar processamento Spark ao conjuntos de dados na arquitetura medalhão do pipeline.
    - **Benefício**: Unir velocidade, escalabilidade e flexibilidade, sem abrir mão da produtividade que o Python oferece.

4. **Delta Live Tables**:
    - **Objetivo**: Utilizar DLT para pipeline de dados na arquitetura medalhão.
    - **Benefício**: Pipelines de dados mais rápidos de desenvolver, mais fáceis de manter e mais resilientes.

5. **Orquestração Unificada**:
    - **Objetivo**: Utilizar ADF para orquestrar o fluxo de dados end to end.
    - **Benefício**: Centraliza, automatiza e monitora todo o ciclo de vida do pipeline de dados, garantindo eficiência, confiabilidade e escalabilidade.

6. **Monitoramento de Processos**:
    - **Objetivo**: Monitoramento de dos recurtsos e status do projeto com Grafna.
    - **Benefício**: Monitoramento de todas as funcionalidades e recursos da arquitetura do projeto em um único local.


## Construção do Ambiente com Terraform:

A estrutura do Terraform é referente ao modelo abaixo:

terraform/
│
├── .terraform/                 # Pasta interna do Terraform (plugins, cache, metadados)
├── .gitignore                   # Arquivo para ignorar arquivos/pastas no controle de versão
├── .terraform.lock.hcl          # Lock file com versões exatas de provedores usados
├── main.tf                      # Arquivo principal com recursos e definições da infraestrutura
├── providers.tf                 # Configuração dos provedores (ex.: Azure, AWS, GCP)
├── terraform.tfstate            # Estado atual da infraestrutura gerenciado pelo Terraform
├── terraform.tfstate.backup     # Backup do estado anterior
├── variables.tf                 # Declaração de variáveis utilizadas nos módulos e recursos
└── versions.tf                  # Definição da versão do Terraform e restrições de provedores

Para executar a construção do ambiente basta executar os seguintes comandos:

1- Entre na pasta com os arquivo terraform:

`cd terraform`

2- Inicie o Terraform:

`terraform init`

3- Valide o Terraform:

`terraform validate`

4- Planeje a construção do Terraform:

`terraform plan`

5- Implemente a construção do Terraform:

`terraform apply -auto-approve`

Ao final da execução teremos os seguintes recursos cloud na Azure:

<div align="center">
  <img src="https://github.com/user-attachments/assets/f8ec74c9-ee08-4101-b8ce-8ad4d3b2cbf7" alt="Dashboard Cloud">
  <p><b>Dashboard Infra</b></p>
</div>

<p align="left">
</p>

## Configuração Manual:

Para quea  criação de Volumes e External Tables estejam habilitadas, é necessário realizar a confgiuração do access conncetor do databricks.

1- Vá para o recurso do access conector e copie o ID do recurso

2- Abra o catalog do databricks e vá em external databricks

3- Crie uma External Credencial usando o ID do recurso

4- Crie uma External Location usando a external Credencial criada e o caminho do storageaccount, no caso:
abfss://steam@steamstorageaccount.dfs.core.windows.net/

Feito esse passo a passo, é possivelc riar volumes e external tables no unity catalog

## Orquestradores e Monitoramento

<div align="center">
  <img src="https://github.com/user-attachments/assets/a91183c5-6890-487c-91a4-872e6d34fccf" alt="Pipeline DLT">
  <p><b>Dashboard DLT</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/c0039453-4bf3-4408-9a45-37a600d51840" alt="Pipeline ADF">
  <p><b>Pipeline ADF</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/7f206935-1365-4baf-a50c-46cb1a5544be" alt="Grafana ADF">
  <p><b>Grafana ADF</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/8c397a52-19be-43a3-a6d8-43b8ab990888" alt="Grafana AZ Function">
  <p><b>Grafana AZ Function</b></p>
</div>

<p align="left">
</p>

<div align="center">
  <img src="https://github.com/user-attachments/assets/24da45cc-0a75-4c9d-a695-7cb96735112c" alt="Grafana Storage">
  <p><b>Grafana Storage</b></p>
</div>

<p align="left">
</p>




