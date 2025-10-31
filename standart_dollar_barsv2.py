#!/usr/bin/env python3
"""
Standard Dollar Bars Generator - Versão Integrada

Este módulo gera "standard dollar bars" a partir de dados de trades do Bitcoin.
Usa processamento distribuído com Dask e otimizações com Numba para processar
grandes volumes de dados eficientemente.
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

def setup_dask_client(n_workers=10, threads_per_worker=1, memory_limit='6GB'):
    """Configura o cliente Dask para processamento distribuído."""
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker, memory_limit=memory_limit)
    client = Client(cluster)
    logging.info(f"Dask client inicializado: {client}")
    return client

def get_data_path(data_type='futures', futures_type='um', granularity='daily'):
    """
    Constrói o caminho para os dados de forma portável, relativo ao script.
    """
    # Tenta encontrar a raiz do projeto subindo um nível
    project_root = Path(__file__).resolve().parent.parent

    # Se o diretório 'datasets' não for encontrado, usa um caminho alternativo
    data_dir = project_root / 'datasets'
    if not data_dir.exists():
        data_dir = Path.home() / "Desktop/hub/degen-ml-finance/datasets"

    if data_type == 'spot':
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / 'spot'
    else:
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / f'futures-{futures_type}'


def read_parquet_files_optimized(raw_dataset_path, file):
    """
    Lê arquivos Parquet de forma otimizada, selecionando colunas e tipos.
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
        }
    )
    return df_dask

def process_partition_data(df):
    """
    Atribui 'side' com base na mudança de preço e calcula 'net_volumes'
    para uma única partição de dados (DataFrame Pandas).
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
        types.ListType(types.Tuple((
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        ))),
        # 2. Estado Final do Sistema: uma tupla com 12 elementos
        types.Tuple((
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        )),
    ))(
        # Assinaturas dos Argumentos de Entrada
        types.float64[:],       # prices
        types.float64[:],       # times
        types.float64[:],       # net_volumes
        types.int8[:],          # sides
        types.float64[:],       # qtys
        # Estado do sistema (12 elementos)
        types.Tuple((
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        )),
        types.float64           # init_vol (limiar fixo)
    )
)
def process_partition_numba(
    prices, times, net_volumes, sides, qtys, system_state, init_vol
):
    """Processa uma partição de dados com Numba para gerar as barras."""
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
            bar_start_time, bar_end_time = np.nan, np.nan
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
    times = partition['time'].values.astype(np.float64)
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


def batch_create_dollar_bars_optimized(df_dask, system_state, init_vol):
    """Processa partições em lote para criar as barras."""
    results = []
    for partition_num in range(df_dask.npartitions):
        logging.info(f'Processando partição {partition_num + 1} de {df_dask.npartitions}')
        part = df_dask.get_partition(partition_num).compute()

        bars, system_state = create_dollar_bars_numba(
            part, system_state, init_vol
        )
        if not bars.empty:
            results.append(bars)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame(), system_state


def process_files_and_generate_bars(
    data_type='futures', futures_type='um', granularity='daily',
    init_vol=40_000_000, output_dir=None, db_engine=None
):
    """Função principal que orquestra todo o processo."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_dataset_path = get_data_path(data_type, futures_type, granularity)
    logging.info(f"Caminho dos dados: {raw_dataset_path}")

    if not raw_dataset_path.exists():
        logging.error(f"Diretório de dados não encontrado: {raw_dataset_path}")
        return

    files = sorted([f for f in os.listdir(raw_dataset_path) if f.endswith('.parquet')])
    if not files:
        logging.error("Nenhum arquivo Parquet encontrado!")
        return
    logging.info(f"Encontrados {len(files)} arquivos Parquet")

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
    system_state = (
        np.nan, -np.inf, np.inf, np.nan, np.nan, np.nan, # bar_open, high, low, close, start_time, end_time (6)
        0.0, 0.0, 0.0, 0.0,                            # current_imbalance, buy_volume_usd, total_volume_usd, total_volume (4)
        0.0, 0.0                                       # ticks, ticks_buy (2)
    )

    results = []

    for i, file in enumerate(files):
        logging.info(f"Processando arquivo {i + 1}/{len(files)}: {file}")
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
                bars_to_db['end_time'] = pd.to_datetime(bars_to_db['end_time'], unit='ns')
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
            logging.info(f"{len(bars)} barras geradas. Tempo: {elapsed_time:.2f} min.")
        else:
            logging.warning(f"Nenhuma barra gerada para o arquivo {file}")

    if results:
        all_bars = pd.concat(results, ignore_index=True)
        all_bars['end_time'] = pd.to_datetime(all_bars['end_time'], unit='ns')
        all_bars.drop(columns=['start_time'], inplace=True)

        final_path = output_dir / f'{output_file_prefix_parquet}.parquet'
        all_bars.to_parquet(final_path, index=False)
        logging.info(f"\nProcessamento completo! Arquivo final salvo em: {final_path}")
        logging.info(f"Total de barras no arquivo final: {len(all_bars)}")

if __name__ == '__main__':
    data_type = 'futures'
    futures_type = 'um'
    granularity = 'daily'
    output_dir = './output/standart/'

    setup_logging()
    client = setup_dask_client(n_workers=10, threads_per_worker=1, memory_limit='6GB')

    # Descomente a linha abaixo para habilitar a gravação no banco de dados
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
            logging.info("Dispondo do engine do banco de dados.")
            engine.dispose()