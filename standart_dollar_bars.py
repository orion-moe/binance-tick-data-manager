#!/usr/bin/env python3
"""
Imbalance Dollar Bars Generator - Versão Integrada

Este módulo gera "imbalance dollar bars" a partir de dados de trades do Bitcoin.
Usa processamento distribuído com Dask e otimizações com Numba para processar
grandes volumes de dados eficientemente.
"""

import os
# import argparse
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

engine = create_engine(db_url)

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
    project_root = Path(__file__).parent.parent # Sobe um nível para a raiz do projeto

    # Se o diretório 'datasets' não for encontrado, usa um caminho padrão
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
    A coluna 'isBuyerMaker' é essencial para o cálculo de imbalance.
    """
    parquet_pattern = os.path.join(raw_dataset_path, file)
    df_dask = dd.read_parquet(
        parquet_pattern,
        columns=['price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'], # 'isBuyerMaker' é crucial
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
    # 1. Atribui o lado da negociação
    df['side'] = np.where(df['price'].shift() > df['price'], 1,
                          np.where(df['price'].shift() < df['price'], -1, np.nan))

    # Preenche os valores nulos (gerados pelo shift() e por preços iguais)
    df['side'] = df['side'].ffill().fillna(1).astype('int8')

    # 2. Calcula os volumes líquidos (net_volumes)
    df['net_volumes'] = df['quoteQty'] * df['side']

    return df


def apply_operations_optimized(df_dask, meta):
    """
    Aplica as operações de processamento em cada partição do Dask DataFrame.
    """
    # Agora, a função `process_partition_data` retorna um DataFrame que corresponde
    # exatamente à estrutura definida em `meta`, incluindo 'net_volumes'.
    return df_dask.map_partitions(process_partition_data, meta=meta)

# ==============================================================================
# SEÇÃO DO ALGORITMO DE GERAÇÃO DE BARRAS (NUMBA)
# ==============================================================================

@njit(
    types.Tuple((
        # Assinatura de retorno para a lista de barras (14 elementos por barra)
        types.ListType(types.Tuple((
            types.float64, types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64
        ))),
        # Assinatura de retorno para o estado final do sistema (15 elementos)
        types.Tuple((
            types.float64, types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64, types.boolean
        )),
    ))(
        # Assinaturas dos argumentos de entrada
        types.float64[:],       # prices
        types.float64[:],       # times
        types.float64[:],       # net_volumes
        types.int8[:],          # sides
        types.float64[:],       # qtys
        types.float64,          # alpha_ticks
        types.float64,          # alpha_imbalance
        types.Tuple((           # system_state (15 elementos)
            types.float64, types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64, types.float64,
            types.float64, types.float64, types.float64, types.float64, types.boolean
        )),
        types.float64,          # init_ticks
        types.float64           # time_reset (NOVO)
    )
)
def process_partition_imbalance_numba(
    prices, times, net_volumes, sides, qtys, alpha_ticks, alpha_imbalance, system_state, init_ticks, time_reset
):
    """Processa uma partição de dados com Numba para gerar as barras."""
    bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
    current_imbalance, buy_volume_usd, total_volume_usd, total_volume, \
    ticks, ticks_buy, ewma_T, ewma_imbalance, warm = system_state

    if warm:
        threshold = init_ticks
    else:
        threshold = ewma_T * ewma_imbalance

    # Converte time_reset (em horas) para nanosegundos para comparação
    # 1 hora = 3.600 segundos = 3.6e12 nanosegundos
    time_reset_ns = 3_600_000.0 * 1_000_000.0 * time_reset

    bars = List()

    for i in range(len(prices)):
        ticks += 1
        # print(times[i])
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

        time_since_bar_start = times[i] - bar_start_time

        # A variável de gatilho muda dependendo se estamos no período 'warm'
        if warm:
            trigger_var = ticks
        else:
            trigger_var = abs(current_imbalance)

        if trigger_var >= threshold:
            bar_end_time = times[i]

            # Atualiza os EWMAs. A primeira atualização é uma simples atribuição.
            if warm:
                ewma_T = ticks
                ewma_imbalance = abs(current_imbalance) / ticks
                warm = False
            else:
                ewma_T += alpha_ticks * (ticks - ewma_T)
                ewma_imbalance += alpha_imbalance * (trigger_var / ticks - ewma_imbalance)

            bars.append((
                bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                current_imbalance, buy_volume_usd, total_volume_usd, total_volume,
                ticks, ticks_buy, ewma_T, ewma_imbalance
            ))

            # Reseta o estado para a próxima barra
            bar_open, bar_high, bar_low, bar_close = np.nan, -np.inf, np.inf, np.nan
            bar_start_time, bar_end_time = np.nan, np.nan
            current_imbalance, buy_volume_usd, total_volume_usd, total_volume, ticks, ticks_buy = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

            # Atualiza o threshold para a próxima barra.
            # Se a barra demorou mais que `time_reset_ns`, reseta para o estado inicial.
            if time_since_bar_start > time_reset_ns:
                warm = True
                threshold = init_ticks
            else:
                threshold = ewma_T * ewma_imbalance

    final_state = (
        bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time,
        current_imbalance, buy_volume_usd, total_volume_usd, total_volume,
        ticks, ticks_buy, ewma_T, ewma_imbalance, warm
    )
    return bars, final_state

# ==============================================================================
# SEÇÃO DE ORQUESTRAÇÃO E PROCESSAMENTO PRINCIPAL
# ==============================================================================

def create_imbalance_dollar_bars_numba(partition, system_state, alpha_ticks, alpha_imbalance, init_ticks, time_reset):
    """Função wrapper para processar uma partição com Numba."""
    prices = partition['price'].values.astype(np.float64)
    times = partition['time'].values.astype(np.float64)
    net_volumes = partition['net_volumes'].values.astype(np.float64)
    sides = partition['side'].values.astype(np.int8)
    qtys = partition['qty'].values.astype(np.float64)

    bars, system_state = process_partition_imbalance_numba(
        prices, times, net_volumes, sides, qtys,
        alpha_ticks, alpha_imbalance, system_state, init_ticks, time_reset
    )

    if bars:
        df_bars = pd.DataFrame(bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'theta_k', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume',
            'ticks', 'ticks_buy', 'ewma_T', 'ewma_imbalance'
        ])
    else:
        df_bars = pd.DataFrame()
    return df_bars, system_state


def batch_create_imbalance_dollar_bars_optimized(df_dask, system_state, alpha_ticks, alpha_imbalance, init_ticks, time_reset):
    """Processa partições em lote para criar as barras."""
    results = []
    for partition_num in range(df_dask.npartitions):
        logging.info(f'Processando partição {partition_num + 1} de {df_dask.npartitions}')
        part = df_dask.get_partition(partition_num).compute()

        bars, system_state = create_imbalance_dollar_bars_numba(
            part, system_state, alpha_ticks, alpha_imbalance, init_ticks, time_reset
        )
        if not bars.empty:
            results.append(bars)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame(), system_state


def process_files_and_generate_bars(
    data_type='futures', futures_type='um', granularity='daily',
    init_ticks=1_000, alpha_ticks=0.9,
    alpha_imbalance=0.9, output_dir=None, time_reset=5.0, db_engine=None
):
    """Função principal que orquestra todo o processo."""
    output_dir = Path(output_dir)

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
    output_file_prefix = f'{data_type}-ticks{init_ticks}-aticks{alpha_ticks}-aimb{alpha_imbalance}-treset{time_reset}'
    output_file_prefix_parquet = f'{timestamp}-imbalance-{data_type}-ticks{init_ticks}-aticks{alpha_ticks}-aimb{alpha_imbalance}-treset{time_reset}'

    # O estado inicial precisa ter 15 elementos, incluindo 'warm'
    system_state = (
        np.nan, -np.inf, np.inf, np.nan, np.nan, np.nan, # bar_open, high, low, close, start_time, end_time (6)
        0.0, 0.0, 0.0, 0.0, # current_imbalance, buy_volume_usd, total_volume_usd, total_volume (4)
        0.0, 0.0,           # ticks, ticks_buy (2)
        float(init_ticks), 0.0, # ewma_T, ewma_imbalance (2) - ewma_imbalance começa em 0.0
        True # warm (1)
    )

    results = []

    for i, file in enumerate(files):
        logging.info(f"Processando arquivo {i + 1}/{len(files)}: {file}")
        start_time_file = time.time()

        df_dask = read_parquet_files_optimized(str(raw_dataset_path), file)
        df_dask = apply_operations_optimized(df_dask, meta)

        bars, system_state = batch_create_imbalance_dollar_bars_optimized(
            df_dask, system_state, alpha_ticks, alpha_imbalance, init_ticks, time_reset
        )
        # print(bars)
        if not bars.empty:
            results.append(bars)
            if db_engine:
                bars['end_time'] = pd.to_datetime(bars['end_time'], unit='ns')
                bars.drop(columns=['start_time'], inplace=True)
                bars['type'] = 'Imbalance'
                bars['sample_date'] = timestamp
                bars['sample'] = output_file_prefix
                with db_engine.connect() as conn:
                    bars.to_sql(
                        name='dollar_bars',  # O nome da sua tabela no PostgreSQL
                        con=conn,             # A conexão que o pandas deve usar
                        if_exists='append',     # O que fazer se a tabela já existir: 'append' = adicionar
                        index=False             # NÃO criar uma coluna para o índice do DataFrame
                        )
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
    output_dir = './output/'

    params = [[init_ticks, alpha_ticks/100, alpha_imbalance/100, time_reset]
                for init_ticks in range(1000, 1200, 200)
                for alpha_ticks in range(10, 100, 30)
                for alpha_imbalance in range(10, 100, 30)
                for time_reset in range(1, 11)]

    setup_logging()
    client = setup_dask_client(n_workers=10, threads_per_worker=1, memory_limit='6GB')

    # engine = create_engine(db_url)
    engine = None

    try:
        for init_ticks, alpha_ticks, alpha_imbalance, time_reset in params:
            print(f'# SAMPLE Ticks0 = {init_ticks} - Alpha ticks = {alpha_ticks} - Alpha imbalance = {alpha_imbalance} - Time reset = {time_reset}')

            start_time_sample = time.time()

            process_files_and_generate_bars(
                data_type=data_type, futures_type=futures_type, granularity=granularity,
                init_ticks=init_ticks, alpha_ticks=alpha_ticks, alpha_imbalance=alpha_imbalance,
                output_dir=output_dir, time_reset=time_reset, db_engine=engine
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