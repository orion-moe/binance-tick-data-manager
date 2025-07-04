# Bitcoin ML Finance Pipeline

Sistema avançado de processamento e análise de dados de trading de Bitcoin com pipeline completo para machine learning.

## 🚀 Features

- **Pipeline Completo**: Download → Extração → Conversão → Otimização → Validação → Features
- **Download Automatizado**: Dados spot e futures da Binance com verificação de integridade
- **Processamento Otimizado**: Conversão para Parquet com tipos otimizados
- **Validação de Integridade**: Verificação completa com relatórios detalhados
- **Engenharia de Features**: Imbalance dollar bars e indicadores avançados
- **Performance**: Processamento distribuído com Dask e otimização com Numba

## 📋 Estrutura do Projeto

```
degen-ml-finance/
├── src/
│   ├── data_pipeline/
│   │   ├── downloaders/      # Download de dados da Binance
│   │   ├── extractors/       # Extração de arquivos CSV
│   │   ├── converters/       # Conversão CSV → Parquet
│   │   ├── processors/       # Otimização e processamento
│   │   └── validators/       # Validação de integridade
│   ├── features/             # Engenharia de features
│   ├── notebooks/            # Análises em Jupyter
│   └── utils/                # Utilitários
├── datasets/                 # Dados baixados
│   ├── dataset-raw-*/        # Arquivos ZIP/CSV
│   └── dataset-raw-*-compressed/  # Arquivos Parquet
├── data/
│   ├── optimized/            # Parquet otimizados
│   └── optimized-filled/     # Com dias faltantes preenchidos
├── main.py                   # Entry point principal
└── requirements.txt          # Dependências
```

## 🛠️ Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/degen-ml-finance.git
cd degen-ml-finance

# Crie um ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Instale as dependências
pip install -r requirements.txt
```

## 🎯 Uso Rápido

### Modo Interativo (Recomendado)

```bash
python main.py
```

O modo interativo guia você através de todo o pipeline:

1. **Seleção de Mercado**: Escolha símbolo, tipo (spot/futures) e granularidade
2. **Pipeline Sequencial**: Execute cada etapa com indicadores de status
3. **Verificação Automática**: Validação em cada passo

### Modo Linha de Comando

```bash
# Download de dados
python main.py download --start 2024-01-01 --end 2024-01-31

# Otimizar arquivos parquet
python main.py optimize --source datasets/raw --target data/optimized

# Validar dados
python main.py validate --quick

# Gerar features
python main.py features --type imbalance
```

## 📊 Pipeline Detalhado

### 1. Download de Dados (✅ Hash Verification)

```python
# Download com verificação de checksum
python src/data_pipeline/downloaders/binance_downloader.py
```

- Download paralelo com múltiplos workers
- Verificação SHA256 de todos os arquivos
- Resume de downloads interrompidos
- Suporte para spot e futures (USD-M e COIN-M)

### 2. Extração de CSV (✅ Integrity Check)

```python
# Extrair e verificar arquivos CSV
python src/data_pipeline/extractors/csv_extractor.py
```

- Extração segura de arquivos ZIP
- Verificação de integridade dos CSV
- Limpeza opcional de ZIPs após extração
- Retry automático para falhas

### 3. Conversão para Parquet (✅ Type Optimization)

```python
# Converter preservando nomenclatura mensal
python src/data_pipeline/converters/csv_to_parquet.py
```

- Preserva nomenclatura baseada em meses
- Otimização de tipos (float32 para preços)
- Compressão Snappy para eficiência
- Suporte para CSVs com/sem headers

### 4. Otimização de Arquivos

```python
# Combinar em arquivos de 10GB
python src/data_pipeline/processors/parquet_optimizer.py
```

- Combina arquivos pequenos em chunks maiores
- Mantém ordem cronológica
- Otimização com Numba JIT
- Reduz número de arquivos para melhor I/O

### 5. Preenchimento de Dias Faltantes

```python
# Identificar e preencher gaps
python src/data_pipeline/processors/missing_days_filler.py
```

- Detecta dias de trading faltantes
- Exclui fins de semana e feriados
- Preenche com dados placeholder
- Garante série temporal contínua

### 6. Validação de Dados

```python
# Validação rápida
python src/data_pipeline/validators/quick_validator.py

# Validação avançada com relatórios
python src/data_pipeline/validators/advanced_validator.py
```

- Verificação de integridade de arquivos
- Análise de qualidade de dados
- Relatórios detalhados em HTML/JSON
- Detecção de anomalias

### 7. Geração de Features

```python
# Gerar imbalance dollar bars
python src/features/imbalance_bars.py
```

- Imbalance bars baseadas em volume dollar
- Cálculo de direção de mudança de preço
- Features de microestrutura de mercado
- Processamento distribuído com Dask

## 📈 Exemplos de Código

### Download Completo para 2024

```python
from datetime import datetime
from src.data_pipeline.downloaders.binance_downloader import BinanceDataDownloader

# Configurar downloader
downloader = BinanceDataDownloader(
    symbol="BTCUSDT",
    data_type="spot",
    granularity="monthly"
)

# Download de todo 2024
start = datetime(2024, 1, 1)
end = datetime(2024, 12, 31)
downloader.download_date_range(start, end, max_workers=10)
```

### Pipeline Completo Automatizado

```python
# Pipeline completo para um símbolo
def run_complete_pipeline(symbol="BTCUSDT", year=2024):
    # 1. Download
    downloader = BinanceDataDownloader(symbol=symbol)
    downloader.download_date_range(
        datetime(year, 1, 1), 
        datetime(year, 12, 31)
    )
    
    # 2. Extrair CSV
    extractor = CSVExtractor(symbol=symbol)
    extractor.extract_and_verify_all()
    
    # 3. Converter para Parquet
    converter = CSVToParquetConverter(symbol=symbol)
    converter.convert_all_csv_files(cleanup_csv=True)
    
    # 4. Otimizar
    # ... (executar otimizador)
    
    # 5. Preencher dias faltantes
    filler = MissingDaysFiller(symbol=symbol)
    filler.fill_all_missing_days()
    
    # 6. Validar
    # ... (executar validadores)
    
    # 7. Gerar features
    # ... (executar gerador de features)
```

## ⚙️ Configuração

### Variáveis de Ambiente

```bash
# Base directory para dados (opcional)
export DEGEN_ML_BASE_DIR=/path/to/data

# Número de workers para download (opcional)
export DEGEN_ML_WORKERS=10

# Nível de log (opcional)
export DEGEN_ML_LOG_LEVEL=INFO
```

### Arquivos de Progresso

O pipeline mantém arquivos de progresso para retomar operações:

- `download_progress_*.json` - Arquivos baixados
- `extraction_progress_*.json` - Arquivos extraídos
- `conversion_progress_*.json` - Arquivos convertidos

## 🧪 Testes

```bash
# Executar todos os testes
python -m pytest tests/

# Testes com coverage
python -m pytest --cov=src tests/

# Testes específicos
python -m pytest tests/test_downloader.py
```

## 🐛 Troubleshooting

### Problemas Comuns

1. **Falhas de Download**
   - Verifique conexão com internet
   - Binance pode ter limites de rate
   - Use menos workers se necessário

2. **Erros de Memória**
   - Reduza chunk_size no processamento
   - Use Dask para processamento distribuído
   - Processe em batches menores

3. **Espaço em Disco**
   - Cada mês de dados ~5-10GB
   - Use compressão Parquet
   - Limpe arquivos intermediários

4. **Dados Faltantes**
   - Fins de semana não têm trading 24/7
   - Alguns feriados podem ter gaps
   - Use o preenchedor de dias faltantes

### Logs Detalhados

Verifique logs em:
- `datasets/logs/` - Logs do pipeline
- `reports/` - Relatórios de validação

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## 🙏 Agradecimentos

- Binance por disponibilizar dados históricos
- Comunidade Python por ferramentas incríveis
- Dask e Numba por processamento de alta performance
- PyArrow/Parquet por armazenamento eficiente