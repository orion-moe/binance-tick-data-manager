# Compressão Parquet - Guia Completo

## ✅ RESPOSTA RÁPIDA

**SIM, a compressão é definida uma vez e reutilizada automaticamente em TODOS os arquivos Parquet!**

Uma vez que você escolhe **Snappy** (que já está configurado), todos os Parquet files usarão Snappy automaticamente:
- ✅ `raw-parquet-daily/` → Snappy
- ✅ `raw-parquet-monthly/` → Snappy
- ✅ `raw-parquet-merged/` → Snappy

**Você NÃO precisa especificar a compressão novamente!**

---

## 🔧 Compressão Configurada: Snappy

### Onde está configurado?

**Arquivo**: `src/data_pipeline/downloaders/binance_downloader.py`

```python
# Linha 619 - Criação de arquivos Parquet individuais
writer = pq.ParquetWriter(output_path, table.schema, compression='snappy')

# Linha 1573 - Merge de arquivos Parquet
writer = pq.ParquetWriter(last_optimized, schema, compression='snappy')

# Linha 1581 - Criação de novos arquivos merged
writer = pq.ParquetWriter(current_output, schema, compression='snappy')
```

**Resultado**: Todos os Parquet files usam Snappy!

---

## 📊 Como Funciona a Compressão Parquet

### Compressão é uma Propriedade do Arquivo

Quando você cria um arquivo Parquet:

```python
writer = pq.ParquetWriter(
    'arquivo.parquet',
    schema,
    compression='snappy'  # ← Definido aqui, uma única vez
)
```

A compressão fica **embutida no arquivo**. Qualquer ferramenta que ler o arquivo:
- ✅ Detecta automaticamente que é Snappy
- ✅ Descomprime automaticamente ao ler
- ✅ **NÃO precisa** especificar compressão ao ler

### Exemplo Prático

```python
# ESCREVER (você especifica)
import pyarrow.parquet as pq
table = pa.Table.from_pandas(df)
pq.write_table(table, 'dados.parquet', compression='snappy')

# LER (automático, não precisa especificar!)
df = pd.read_parquet('dados.parquet')  # Descomprime sozinho!
```

---

## 🎯 Por Que Usamos Snappy?

### Comparação de Compressões

| Compressão | Velocidade Leitura | Velocidade Escrita | Taxa Compressão | Tamanho Final |
|------------|-------------------|-------------------|----------------|---------------|
| **Snappy** | ⚡⚡⚡ 500 MB/s | ⚡⚡⚡ 250 MB/s | ~3x | 100 MB → 33 MB |
| GZIP | 🐌 100 MB/s | 🐌 20 MB/s | ~10x | 100 MB → 10 MB |
| LZ4 | ⚡⚡⚡⚡ 700 MB/s | ⚡⚡⚡ 300 MB/s | ~2x | 100 MB → 50 MB |
| Zstd | ⚡⚡ 300 MB/s | ⚡⚡ 100 MB/s | ~7x | 100 MB → 14 MB |
| None | ⚡⚡⚡⚡⚡ 1000 MB/s | ⚡⚡⚡⚡⚡ 900 MB/s | 1x | 100 MB → 100 MB |

### Por Que Snappy é Ideal para ML?

**Caso de Uso: Treinar modelo com 100GB de dados**

| Compressão | Tempo Leitura | Tamanho em Disco | Velocidade Treinamento |
|------------|--------------|------------------|----------------------|
| None | 2 min | 100 GB | ⚡ Muito Rápido |
| **Snappy** | **3 min** | **33 GB** | **⚡ Rápido** ← **MELHOR** |
| GZIP | 15 min | 10 GB | 🐌 Lento |
| Zstd | 6 min | 14 GB | ⚡ Médio |

**Snappy vence porque:**
1. ✅ Economiza 67% de espaço em disco
2. ✅ Adiciona apenas 50% no tempo de leitura
3. ✅ Não sacrifica velocidade de treinamento
4. ✅ É o padrão da indústria para ML

---

## 🔄 Fluxo de Compressão no Pipeline

### Etapa 1: Download (ZIP da Binance)

```
Binance Server
     ↓
raw-zip-daily/BTCUSDT-trades-2024-01-15.zip (200 MB)
```

**Compressão**: ZIP (compressão da Binance)

---

### Etapa 2: Conversão para Parquet (Snappy aplicado)

```python
# binance_downloader.py:619
writer = pq.ParquetWriter(output_path, table.schema, compression='snappy')
```

```
raw-zip-daily/BTCUSDT-trades-2024-01-15.zip (200 MB)
     ↓ [Extrai CSV]
     ↓ [Converte para Parquet + Snappy]
raw-parquet-daily/BTCUSDT-Trades-2024-01-15.parquet (65 MB)
```

**Compressão**: Snappy (definida aqui)

---

### Etapa 3: Merge (Snappy mantido)

```python
# binance_downloader.py:1581
writer = pq.ParquetWriter(current_output, schema, compression='snappy')
```

```
raw-parquet-daily/
├── BTCUSDT-Trades-2024-01-01.parquet (65 MB, snappy)
├── BTCUSDT-Trades-2024-01-02.parquet (65 MB, snappy)
├── ...
└── BTCUSDT-Trades-2024-12-31.parquet (65 MB, snappy)
     ↓ [Merge com Snappy]
raw-parquet-merged/BTCUSDT-Trades-Optimized-001.parquet (10 GB, snappy)
```

**Compressão**: Snappy (mesma)

---

## ✅ Garantias

### 1. Consistência Total

✅ **TODOS** os arquivos Parquet usam Snappy
✅ Não há mistura de compressões
✅ Pipeline totalmente consistente

### 2. Automático

✅ Definido uma vez no código
✅ Reutilizado automaticamente
✅ Você **NÃO** precisa pensar nisso!

### 3. Compatibilidade

✅ Qualquer ferramenta Parquet lê corretamente:
- ✅ Pandas: `pd.read_parquet()`
- ✅ Dask: `dd.read_parquet()`
- ✅ PyArrow: `pq.read_table()`
- ✅ Spark: `spark.read.parquet()`
- ✅ DuckDB: `SELECT * FROM parquet_scan()`

Todas detectam Snappy automaticamente!

---

## 🧪 Como Verificar a Compressão

### Verificar arquivo Parquet:

```python
import pyarrow.parquet as pq

# Ler metadados
pf = pq.ParquetFile('data/btcusdt-spot/raw-parquet-daily/BTCUSDT-Trades-2024-01-15.parquet')

# Ver compressão
for i in range(pf.metadata.num_row_groups):
    rg = pf.metadata.row_group(i)
    for j in range(rg.num_columns):
        col = rg.column(j)
        print(f"Column {col.path_in_schema}: {col.compression}")
```

**Output esperado**:
```
Column trade_id: SNAPPY
Column price: SNAPPY
Column qty: SNAPPY
...
```

### Via Terminal:

```bash
# Instalar parquet-tools
pip install parquet-tools

# Ver metadados
parquet-tools meta data/btcusdt-spot/raw-parquet-daily/BTCUSDT-Trades-2024-01-15.parquet

# Output mostrará: compression: SNAPPY
```

---

## 🔧 Se Precisar Mudar a Compressão

### Cenário: Você quer usar GZIP em vez de Snappy

**Arquivo**: `binance_downloader.py`

```python
# Substituir em 3 lugares:

# Linha 619
writer = pq.ParquetWriter(output_path, table.schema, compression='gzip')

# Linha 1573
writer = pq.ParquetWriter(last_optimized, schema, compression='gzip')

# Linha 1581
writer = pq.ParquetWriter(current_output, schema, compression='gzip')
```

**Mas NÃO recomendamos!** Snappy é ideal para ML.

---

## 📝 Resumo

### Perguntas e Respostas

**Q: A compressão é reutilizada automaticamente?**
A: ✅ SIM! Uma vez definida (`compression='snappy'`), todos os arquivos usam Snappy.

**Q: Preciso especificar ao ler?**
A: ❌ NÃO! Pandas/PyArrow detectam automaticamente.

**Q: Posso misturar compressões?**
A: ⚠️ Tecnicamente sim, mas **não faça isso!** Mantenha consistência.

**Q: Snappy é a melhor escolha?**
A: ✅ SIM para ML! Melhor balanço velocidade/tamanho.

**Q: Os arquivos já existentes usam Snappy?**
A: ✅ SIM! Veja código nas linhas 619, 1573, 1581.

---

## 🎯 Conclusão

### Configuração Atual (Ótima para ML):

```
Pipeline Completo:
├── raw-zip-daily/        (ZIP da Binance)
├── raw-parquet-daily/    (Parquet + Snappy) ✅
└── raw-parquet-merged/   (Parquet + Snappy) ✅
```

**Resultado**:
- ✅ 67% de economia de espaço vs não comprimido
- ✅ 3x menor que CSV
- ✅ Rápido para treinar modelos
- ✅ Totalmente automático
- ✅ Consistente em todo o pipeline

**Você não precisa se preocupar com compressão - já está otimizado!** 🚀
