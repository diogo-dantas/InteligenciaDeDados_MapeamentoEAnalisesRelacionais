# Inteligência de Dados - Estrutura e Análises 📊

> Sistema de gerenciamento de pipeline de dados para um departamento de inteligência de um banco fictício.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Sobre o Projeto

Este repositório contém a estrutura de um banco de dados relacional projetado para suportar as atividades do Departamento de Inteligência de Dados (ID) de um banco, com foco em análise, governança e qualidade de dados. O sistema oferece funcionalidades essenciais como gestão de origens de dados, monitoramento de fluxos, análises avançadas e rastreamento de resultados.

### Estrutura do Banco de Dados (*smart_data_db*)

#### 1. Tabela: dados_origem

Esta tabela armazena informações sobre as diferentes fontes de dados que estão sendo utilizadas.

| Coluna      | Tipo         | Descrição                                       |
| ----------- | ------------ | ----------------------------------------------- |
| id_origem   | SERIAL (PK)  | Identificador único da origem dos dados         |
| nome_origem | VARCHAR(255) | Nome da origem dos dados                        |
| tipo_dado   | VARCHAR(100) | Tipo de dado (ex: transacional, log, etc.)      |
| volume      | INT          | Volume estimado de dados na origem              |
| latencia    | VARCHAR(50)  | Latência de atualização (ex: real-time, diário) |
| descricao   | TEXT         | Descrição detalhada da origem                   |

#### 2. Tabela: fluxo_dados

Esta tabela representa os fluxos de dados desde a origem até o consumo final.

| Coluna           | Tipo         | Descrição                                  |
| ---------------- | ------------ | ------------------------------------------ |
| id_fluxo         | SERIAL (PK)  | Identificador único do fluxo de dados      |
| id_origem        | INT (FK)     | Referência para a origem dos dados         |
| destino          | VARCHAR(255) | Destino do fluxo (ex: analistas, sistemas) |
| status           | VARCHAR(50)  | Status do fluxo (ex: ativo, inativo)       |
| data_criacao     | TIMESTAMP    | Data em que o fluxo foi criado             |
| data_atualizacao | TIMESTAMP    | Data da última atualização do fluxo        |

#### 3. Tabela: analises

Esta tabela armazena as análises realizadas com os dados, incluindo suas hipóteses e resultados.

| Coluna       | Tipo         | Descrição                                  |
| ------------ | ------------ | ------------------------------------------ |
| id_analise   | SERIAL (PK)  | Identificador único da análise             |
| id_fluxo     | INT (FK)     | Referência para o fluxo de dados utilizado |
| hipoteses    | TEXT         | Hipóteses levantadas para a análise        |
| resultado    | TEXT         | Resultados da análise                      |
| data_analise | TIMESTAMP    | Data em que a análise foi realizada        |
| responsavel  | VARCHAR(255) | Nome do responsável pela análise           |



![Diagrama ER - Graphviz](https://github.com/diogo-dantas/InteligenciaDeDados_MapeamentoEAnalisesRelacionais/blob/main/diagrama_er.png)



## 🚀 Funcionalidades

- ✨ Gestão de dados através de PostgreSQL

- 📊 Análise de dados com Pandas

- 🔄 Geração de dados fictícios com Faker

- 📦 Execução de consultas via IPython-SQL

- 📝 Visualização do modelo ER com Graphviz

- ⏰ Testes automatizados via GitHub Actions

## 🛠️ Tecnologias Utilizadas

- PostgreSQL como SGBD principal

- Python com Pandas para análise

- Faker para dados de teste

- IPython-SQL para consultas

- Graphviz para visualização ER

- pytest e unittest.mock para testes

- GitHub Actions para CI

- SQL Avançado
- Window Functions
- CTEs (Common Table Expressions)
- Funções Analíticas

## 📦 Pré-requisitos

- PostgreSQL instalado e configurado
- Python 3.x
- Jupyter Notebook
- Bibliotecas Python: pandas, faker, ipython-sql, graphviz
- pytest e unittest.mock para testes

## 🔧 Instalação

1. Clone o repositório:

```bash
git clone https://github.com/diogo-dantas/InteligenciaDeDados_MapeamentoEAnalisesRelacionais.git
```

2. Entre no diretório do projeto:

```bash
cd InteligenciaDeDados_MapeamentoEAnalisesRelacionais
```

3. Instale as dependências:

```bash
pip install -r requirements.txt
```

4. Configure o banco de dados:

```bash
psql -f setup/database.sql
```

## 🚀 Uso

1. Execute o Jupyter Notebook:

```bash
jupyter notebook
```

2. Configure as variáveis de ambiente:

```bash
cp .env.example .env
nano .env
```

## 🤝 Contribuindo

1. Faça um Fork do projeto

2. Crie sua Feature Branch (`git checkout -b feature/AmazingFeature`)

3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)

4. Push para a Branch (`git push origin feature/AmazingFeature`)

5. Abra um Pull Request

   

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
