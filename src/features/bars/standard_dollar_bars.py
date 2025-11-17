#!/usr/bin/env python3
"""
Standard Dollar Bars Generator - Versão Integrada

This module generates "standard dollar bars" from Bitcoin trade data.
Uses distributed processing with Dask and Numba optimizations to process
large volumes of data efficiently.
"""

import os
import datetime
import logging
import time
from pathlib import Path
import gc

import numpy as np
import pandas as pd
import dask.dataframe as dd

from numba import njit, types
from numba.typed import List
from dask.distributed import Client, LocalCluster

from sqlalchemy import create_engine

# ==============================================================================
# SEÇÃO DE FUNÇÕES UTILITÁRIAS INTEGRADAS
# ==============================================================================

# Pegando as credenciais do ambiente (ou defina diretamente para testar)
host = 'localhost'
port = '5432'
dbname = 'superset'
user = 'superset'
password = 'superset'

# Criar a URL de conexão para o SQLAlchemy
db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

def setup_logging():
    """Configura o sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def setup_dask_client(n_workers=None, threads_per_worker=None, memory_limit=None):
    """Configura o cliente Dask para processamento distribuído com otimização automática de CPU."""
    import psutil
    import multiprocessing

    # Detecta automaticamente o número de CPUs
    cpu_count = multiprocessing.cpu_count()
    available_memory = psutil.virtual_memory().available / (1024**3)  # Em GB

    # Se não especificado, usa configuração otimizada CONSERVADORA
    if n_workers is None:
        # Usa menos workers para evitar contenção (1/3 dos cores, min 2, max 6)
        # Menos workers = mais memória por worker = menos crashes
        n_workers = max(min(cpu_count // 3, 6), 2)

    if threads_per_worker is None:
        # Usa apenas 1 thread por worker para evitar race conditions
        # Processamento sequencial é mais estável para operações pesadas
        threads_per_worker = 1

    if memory_limit is None:
        # Uses 60% da memória disponível dividida entre workers (ainda mais conservador)
        # Deixa 40% livre para sistema operacional e cache de disco
        memory_per_worker = (available_memory * 0.6) / n_workers
        memory_limit = f'{memory_per_worker:.1f}GB'

    logging.info(f"🖥️ CPUs detectadas: {cpu_count}")
    logging.info(f"💾 Memória disponível: {available_memory:.1f} GB")
    logging.info(f"⚙️ Configuração Dask: {n_workers} workers × {threads_per_worker} threads = {n_workers * threads_per_worker} threads totais")
    logging.info(f"📊 Memória por worker: {memory_limit}")

    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        processes=True,  # Uses processos separados para melhor paralelismo
        silence_logs=logging.ERROR,  # Reduz logs do Dask para melhorar performance
        # Configurações adicionais para estabilidade
        death_timeout=600,  # Tempo para considerar worker morto (10 min - aumentado)
        lifetime='30 minutes',  # Recria workers a cada 30 min para evitar memory leaks
        lifetime_stagger='5 minutes',  # Stagger para não recriar todos ao mesmo tempo
    )

    # Configurações de resiliência
    import dask
    dask.config.set({
        'distributed.scheduler.allowed-failures': 5,  # Permite 5 falhas antes de desistir
        'distributed.comm.timeouts.connect': '300s',  # Timeout de conexão
        'distributed.comm.timeouts.tcp': '300s',  # Timeout TCP
        'distributed.scheduler.work-stealing': False,  # DESABILITA work stealing (causa deadlocks)
        'distributed.worker.memory.target': 0.75,  # Começa a liberar memória em 75%
        'distributed.worker.memory.spill': 0.85,  # Spill to disk em 85%
        'distributed.worker.memory.pause': 0.90,  # Pausa worker em 90%
        'distributed.worker.memory.terminate': 0.98,  # Termina worker em 98%
    })

    client = Client(
        cluster,
        # Timeouts aumentados para evitar erros de coleta
        timeout='600s',  # Timeout para operações de rede (aumentado para 10 min)
        heartbeat_interval='60s',  # Intervalo de heartbeat (reduzido overhead)
    )

    # Otimizações adicionais
    client.run(lambda: __import__('gc').collect())  # Força garbage collection em todos workers

    logging.info(f"✅ Dask client otimizado inicializado: {client}")
    return client

def get_data_path(data_type='futures', futures_type='um', granularity='daily'):
    """
    Builds the data path in a portable way, relative to the script.
    Uses the new ticker-based directory structure with backward compatibility.
    """
    # Encontra a raiz do projeto subindo três níveis (src/features/bars -> raiz)
    project_root = Path(__file__).resolve().parent.parent.parent.parent

    # Usa o diretório 'data' na raiz do projeto
    data_dir = project_root / 'data'

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    # Usa a estrutura de compatibilidade com symlinks
    if data_type == 'spot':
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / 'spot'
    else:
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / f'futures-{futures_type}'


def read_parquet_files_optimized(raw_dataset_path, file):
    """
    Reads Parquet files in an optimized way, selecting columns and types.
    Uses larger block size to reduce number of partitions and overhead.
    """
    parquet_pattern = os.path.join(raw_dataset_path, file)
    df_dask = dd.read_parquet(
        parquet_pattern,
        columns=['price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'],
        engine='pyarrow',
        dtype={
            'price': 'float32',
            'qty': 'float32',
            'quoteQty': 'float32',
            'isBuyerMaker': 'bool'
        },
        blocksize='512MB',  # Partições maiores = menos overhead
        aggregate_files=True,  # Agrupa arquivos pequenos
        split_row_groups=False,  # Não divide row groups (mais estável)
    )
    return df_dask

def process_partition_data(df):
    """
    Atribui 'side' com base na mudança de preço e calcula 'net_volumes'
    for a single data partition (Pandas DataFrame).
    """
    df['side'] = np.where(df['price'].shift() > df['price'], 1,
                          np.where(df['price'].shift() < df['price'], -1, np.nan))

    df['side'] = df['side'].ffill().fillna(1).astype('int8')
    df['net_volumes'] = df['quoteQty'] * df['side']
    return df


def apply_operations_optimized(df_dask, meta):
    """
    Aplica as operações de processamento em cada partição do Dask DataFrame.
    """
    return df_dask.map_partitions(process_partition_data, meta=meta)

# ==============================================================================
# SEÇÃO DO ALGORITMO DE GERAÇÃO DE BARRAS (NUMBA)
# ==============================================================================

@njit(
    # Assinatura de Retorno: (Lista de Barras, Estado Final do Sistema)
    types.Tuple((
        # 1. Lista de Barras: cada barra é uma tupla de 12 elementos
        # start_time(int64), end_time(int64), open, high, low, close, theta_k, buy_vol, total_vol_usd, total_vol, ticks, ticks_buy
        types.ListType(types.Tuple((
            types.int64, types.int64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        ))),
        # 2. Estado Final do Sistema: uma tupla com 12 elementos
        # bar_open, high, low, close, start_time(int64), end_time(int64), imbalance, buy_vol, total_vol_usd, total_vol, ticks, ticks_buy
        types.Tuple((
            types.float64, types.float64, types.float64, types.float64,
            types.int64, types.int64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        )),
    ))(
        # Assinaturas dos Argumentos de Entrada
        types.float64[:],       # prices
        types.int64[:],         # times (agora int64 - timestamps em milissegundos)
        types.float64[:],       # net_volumes
        types.int8[:],          # sides
        types.float64[:],       # qtys
        # Estado do sistema (12 elementos)
        types.Tuple((
            types.float64, types.float64, types.float64, types.float64,
            types.int64, types.int64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        )),
        types.float64           # init_vol (limiar fixo)
    )
)
def process_partition_numba(
    prices, times, net_volumes, sides, qtys, system_state, init_vol
):
    """Processes a data partition with Numba to generate bars."""
    bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
    current_imbalance, buy_volume_usd, total_volume_usd, total_volume, \
    ticks, ticks_buy = system_state

    threshold = init_vol
    bars = List()

    for i in range(len(prices)):
        ticks += 1

        if np.isnan(bar_open):
            bar_open = prices[i]
            bar_start_time = times[i]

        trade_price = prices[i]
        bar_high = max(bar_high, trade_price)
        bar_low = min(bar_low, trade_price)
        bar_close = trade_price

        if sides[i] > 0:
            buy_volume_usd += net_volumes[i]
            ticks_buy += 1

        total_volume += qtys[i]
        total_volume_usd += abs(net_volumes[i])
        current_imbalance += net_volumes[i]

        if total_volume_usd >= threshold:
            bar_end_time = times[i]

            bars.append((
                bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                current_imbalance, buy_volume_usd, total_volume_usd, total_volume,
                ticks, ticks_buy
            ))

            # Reseta o estado para a próxima barra
            bar_open, bar_high, bar_low, bar_close = np.nan, -np.inf, np.inf, np.nan
            bar_start_time, bar_end_time = 0, 0  # int64: 0 indica sem valor
            current_imbalance, buy_volume_usd, total_volume_usd, total_volume, ticks, ticks_buy = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

    final_state = (
        bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time,
        current_imbalance, buy_volume_usd, total_volume_usd, total_volume,
        ticks, ticks_buy
    )
    return bars, final_state

# ==============================================================================
# SEÇÃO DE ORQUESTRAÇÃO E PROCESSAMENTO PRINCIPAL
# ==============================================================================

def create_dollar_bars_numba(partition, system_state, init_vol):
    """Função wrapper para processar uma partição com Numba."""
    prices = partition['price'].values.astype(np.float64)
    # Converte datetime64[ms] para timestamp em milissegundos (int64)
    times = partition['time'].values.astype('datetime64[ms]').astype(np.int64)
    net_volumes = partition['net_volumes'].values.astype(np.float64)
    sides = partition['side'].values.astype(np.int8)
    qtys = partition['qty'].values.astype(np.float64)

    bars, system_state = process_partition_numba(
        prices, times, net_volumes, sides, qtys,
        system_state, init_vol
    )

    if bars:
        # A lista de colunas agora tem 12 itens para corresponder à tupla da barra
        df_bars = pd.DataFrame(bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'theta_k', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume',
            'ticks', 'ticks_buy'
        ])
    else:
        df_bars = pd.DataFrame()
    return df_bars, system_state


def batch_create_dollar_bars_optimized(df_dask, system_state, init_vol, max_retries=3):
    """Processes partitions in batch to create bars with retry logic."""
    results = []
    failed_partitions = []

    for partition_num in range(df_dask.npartitions):
        logging.info(f'Processing partition {partition_num + 1} de {df_dask.npartitions}')

        # Retry logic para partições que falham
        for attempt in range(max_retries):
            try:
                part = df_dask.get_partition(partition_num).compute()

                bars, system_state = create_dollar_bars_numba(
                    part, system_state, init_vol
                )
                if not bars.empty:
                    results.append(bars)
                break  # Sucesso, sai do loop de retry

            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f'⚠️ Partition {partition_num + 1} failed (attempt {attempt + 1}/{max_retries}): {e}')
                    logging.info(f'🔄 Retrying partition {partition_num + 1}...')
                    gc.collect()  # Força garbage collection antes de tentar novamente
                    time.sleep(2)  # Aguarda um pouco antes de tentar novamente
                else:
                    logging.error(f'❌ Partition {partition_num + 1} failed after {max_retries} attempts: {e}')
                    failed_partitions.append(partition_num + 1)

    if failed_partitions:
        logging.warning(f'⚠️ {len(failed_partitions)} partitions failed: {failed_partitions}')

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame(), system_state


def process_files_and_generate_bars(
    data_type='futures', futures_type='um', granularity='daily',
    init_vol=40_000_000, output_dir=None, db_engine=None
):
    """Função principal que orquestra todo o processo."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_dataset_path = get_data_path(data_type, futures_type, granularity)
    logging.info(f"Data path: {raw_dataset_path}")

    if not raw_dataset_path.exists():
        logging.error(f"Data directory not found: {raw_dataset_path}")
        return

    files = sorted([f for f in os.listdir(raw_dataset_path) if f.endswith('.parquet')])
    if not files:
        logging.error("No Parquet files found!")
        return
    logging.info(f"Found {len(files)} Parquet files")

    meta = pd.DataFrame({
        'price': pd.Series(dtype='float32'), 'qty': pd.Series(dtype='float32'),
        'quoteQty': pd.Series(dtype='float32'), 'time': pd.Series(dtype='int64'),
        'isBuyerMaker': pd.Series(dtype='bool'), 'side': pd.Series(dtype='int8'),
        'net_volumes': pd.Series(dtype='float64')
    })

    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_file_prefix = f'{data_type}-volume{init_vol}'
    output_file_prefix_parquet = f'{timestamp}-standart-{data_type}-volume{init_vol}'

    # O estado inicial agora tem 12 elementos, sem ewma_T, ewma_imbalance e warm
    # start_time e end_time agora são int64 (0 indica sem valor)
    system_state = (
        np.nan, -np.inf, np.inf, np.nan, 0, 0,  # bar_open, high, low, close, start_time(int64), end_time(int64) (6)
        0.0, 0.0, 0.0, 0.0,                     # current_imbalance, buy_volume_usd, total_volume_usd, total_volume (4)
        0.0, 0.0                                # ticks, ticks_buy (2)
    )

    results = []

    for i, file in enumerate(files):
        logging.info(f"Processing file {i + 1}/{len(files)}: {file}")
        start_time_file = time.time()

        df_dask = read_parquet_files_optimized(str(raw_dataset_path), file)
        df_dask = apply_operations_optimized(df_dask, meta)

        bars, system_state = batch_create_dollar_bars_optimized(
            df_dask, system_state, init_vol
        )

        if not bars.empty:
            results.append(bars)
            if db_engine:
                bars_to_db = bars.copy()
                # Converte timestamps de milissegundos para datetime
                bars_to_db['end_time'] = pd.to_datetime(bars_to_db['end_time'], unit='ms')
                bars_to_db['start_time'] = pd.to_datetime(bars_to_db['start_time'], unit='ms')
                bars_to_db.drop(columns=['start_time'], inplace=True)
                bars_to_db['type'] = 'Standard'
                bars_to_db['sample_date'] = timestamp
                bars_to_db['sample'] = output_file_prefix
                with db_engine.connect() as conn:
                    bars_to_db.to_sql(
                        name='dollar_bars',
                        con=conn,
                        if_exists='append',
                        index=False
                    )
                    conn.commit()
                print("✅ Dados inseridos com sucesso no banco!")

            elapsed_time = (time.time() - start_time_file) / 60
            logging.info(f"{len(bars)} bars generated. Tempo: {elapsed_time:.2f} min.")
        else:
            logging.warning(f"No bars generated for file {file}")

    if results:
        all_bars = pd.concat(results, ignore_index=True)
        # Converte timestamps de milissegundos para datetime
        all_bars['end_time'] = pd.to_datetime(all_bars['end_time'], unit='ms')
        all_bars['start_time'] = pd.to_datetime(all_bars['start_time'], unit='ms')
        all_bars.drop(columns=['start_time'], inplace=True)

        final_path = output_dir / f'{output_file_prefix_parquet}.parquet'
        all_bars.to_parquet(final_path, index=False)
        logging.info(f"\nProcessing complete! Final file saved at: {final_path}")
        logging.info(f"Total bars in final file: {len(all_bars)}")

if __name__ == '__main__':
    data_type = 'futures'
    futures_type = 'um'
    granularity = 'daily'
    output_dir = './output/standart/'

    setup_logging()
    # Usa detecção automática para otimizar uso de CPU e memória
    client = setup_dask_client()

    # Uncomment the line below to enable database writing
    # engine = create_engine(db_url)
    engine = None

    try:
        # Loop para testar diferentes limiares de volume em USD
        for volume_usd_trig in range(40_000_000, 45_000_000, 5_000_000):
            print(f'\n# INICIANDO AMOSTRA COM VOLUME USD: {volume_usd_trig:,}')
            start_time_sample = time.time()

            process_files_and_generate_bars(
                data_type=data_type, futures_type=futures_type, granularity=granularity,
                init_vol=volume_usd_trig, output_dir=output_dir, db_engine=engine
            )

            end_time_sample = time.time()
            logging.info(f"Tempo total da amostra: {(end_time_sample - start_time_sample) / 60:.2f} minutos.")
            logging.info("Forçando a coleta de lixo antes da próxima amostra...")
            gc.collect()
    finally:
        logging.info("Fechando o cliente Dask.")
        client.close()
        if engine is not None:
            logging.info("Disposing database engine.")
            engine.dispose()