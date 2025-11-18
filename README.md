# Binance Tick Data Manager

Pipeline de alta performance para download e processamento de dados de criptomoedas em arquivos Parquet para machine learning.

## Quick Start

### 1. Instalação

```bash
# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instalar dependências
pip install -r requirements.txt
```

### 2. Executar Pipeline

```bash
# Modo interativo (recomendado)
python main.py
```

## Pipeline - Etapas

O pipeline é executado em etapas sequenciais:

### Etapa 1: Download
- Download de ZIPs históricos da Binance
- Verificação de checksum automática
- Suporte a Spot e Futures (USD-M / COIN-M)
- Progresso salvo em `download_progress_daily.json`

### Etapa 2: Conversão
Duas opções disponíveis:
- **Legacy**: ZIP → CSV → Parquet (mais lento, usa mais disco)
- **Otimizado**: ZIP → Parquet direto (streaming, recomendado)

### Etapa 3: Merge/Otimização
- Agrupa arquivos Parquet diários em arquivos maiores (~10GB)
- Compressão Snappy para melhor performance
- Limpeza automática dos arquivos intermediários

### Etapa 4: Validação
- Verificação de datas faltantes
- Validação de integridade dos arquivos Parquet
- Detecção de arquivos corrompidos

### Etapa 5: Features
Geração de barras alternativas para ML:
- **Standard Dollar Bars**: Barras por volume em dólares fixo
- **Imbalance Dollar Bars**: Barras adaptativas baseadas em desequilíbrio

## Estrutura de Diretórios

```
binance-tick-data-manager/
├── main.py                    # Entry point principal
├── src/
│   ├── data_pipeline/         # ETL modules
│   │   ├── downloaders/       # Download da Binance
│   │   ├── extractors/        # Extração CSV
│   │   ├── converters/        # Conversão para Parquet
│   │   ├── processors/        # Merge e otimização
│   │   ├── validators/        # Validação de dados
│   │   └── utils/             # Utilitários
│   ├── features/              # Feature engineering
│   │   └── bars/              # Geração de barras
│   └── scripts/               # Scripts auxiliares
├── data/                      # Dados (por ticker)
│   ├── btcusdt-spot/
│   │   ├── raw-zip-daily/            # ZIPs baixados
│   │   ├── raw-parquet-daily/        # Parquets individuais
│   │   ├── raw-parquet-merged-daily/ # Parquets merged (~10GB)
│   │   ├── output/                   # Features geradas
│   │   └── logs/                     # Logs locais
│   ├── btcusdt-futures-um/           # Futures USD-M
│   └── logs/                         # Logs globais
├── output/                    # Features (organizado por ticker)
└── notebooks/                 # Análises exploratórias
```

## Funcionalidades Implementadas

| Feature | Status | Descrição |
|---------|--------|-----------|
| Download com checksum | ✅ | Verificação SHA256 automática |
| Suporte Spot | ✅ | Dados de mercado spot |
| Suporte Futures | ✅ | USD-M e COIN-M |
| ZIP → Parquet streaming | ✅ | Conversão otimizada sem CSV intermediário |
| Merge de Parquets | ✅ | Agrupa em arquivos ~10GB |
| Validação de datas | ✅ | Detecta gaps nos dados |
| Standard Dollar Bars | ✅ | Barras por volume fixo em dólares |
| Imbalance Dollar Bars | ✅ | Barras adaptativas por desequilíbrio |
| Progress tracking | ✅ | Retoma downloads interrompidos |

## Em Desenvolvimento

| Feature | Status | Descrição |
|---------|--------|-----------|
| Imbalance Bars (tick) | 🔄 | Barras por desequilíbrio de ticks |
| Testes unitários | ⬜ | Suite de testes automatizados |
| Tick Bars | ⬜ | Barras por número de ticks |
| Volume Bars | ⬜ | Barras por volume de contratos |
| CLI arguments | ⬜ | Execução por linha de comando |
| Modelos ML | ⬜ | Integração com frameworks de ML |

## Uso dos Dados

### Leitura de Parquet

```python
import pandas as pd

# Arquivo único
df = pd.read_parquet("data/btcusdt-spot/raw-parquet-merged-daily/merged_part_0.parquet")

# Múltiplos arquivos com Dask (arquivos maiores que RAM)
import dask.dataframe as dd
df = dd.read_parquet("data/btcusdt-spot/raw-parquet-merged-daily/*.parquet")
```

### Leitura de Dollar Bars

```python
import pandas as pd

# Standard Dollar Bars
df = pd.read_parquet("data/btcusdt-spot/output/standard_dollar_bars.parquet")

# Imbalance Dollar Bars
df = pd.read_parquet("output/btcusdt-spot/imbalance_dollar_bars.parquet")
```

## Requisitos

- Python 3.8+
- ~10GB espaço em disco por ano de dados
- Conexão com internet para download da Binance

## Arquitetura

### Princípios de Design

- **Simples**: Sem Docker, databases ou infraestrutura complexa
- **Rápido**: Parquet files para 10x melhor performance que CSV
- **Confiável**: Verificação de checksum e validação de dados
- **Resumível**: Tracking de progresso para operações interrompidas
- **Organizado**: Cada ticker em seu próprio diretório

### Tecnologias

- **PyArrow**: Leitura/escrita de Parquet
- **Dask**: Processamento de arquivos maiores que RAM
- **Pandas**: Manipulação de DataFrames
- **httpx**: Downloads HTTP async

## License

MIT
