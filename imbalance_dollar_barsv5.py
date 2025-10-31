#!/usr/bin/env python3
"""
Imbalance Dollar Bars Generator - Versão Integrada

Este módulo gera "imbalance dollar bars" a partir de dados de trades do Bitcoin.
Usa processamento distribuído com Dask e otimizações com Numba para processar
grandes volumes de dados eficientemente.

- Funções de setup e leitura de dados foram integradas para maior robustez.
- A lógica de cálculo de 'side' baseada em 'isBuyerMaker' foi mantida por ser
  mais precisa para o cálculo de imbalance.
"""

import os
import argparse
import datetime
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
import dask.dataframe as dd

from numba import njit, types
from numba.typed import List
from dask.distributed import Client, LocalCluster

# ==============================================================================
# SEÇÃO DE FUNÇÕES UTILITÁRIAS INTEGRADAS
# ==============================================================================

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
    alpha_imbalance=0.9, output_dir=None, time_reset=5.0
):
    """Função principal que orquestra todo o processo."""
    setup_logging()
    client = setup_dask_client()

    raw_dataset_path = get_data_path(data_type, futures_type, granularity)
    output_dir = Path(output_dir) if output_dir else Path(__file__).parent / 'output'
    output_dir.mkdir(exist_ok=True)

    logging.info(f"Caminho dos dados: {raw_dataset_path}")
    logging.info(f"Diretório de saída: {output_dir}")
    logging.info(f"Ticks iniciais esperados (para a 1ª barra): {init_ticks}")

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
    output_file_prefix = f'{timestamp}-imbalance_dollar_bars_ticks{init_ticks}-aticks{alpha_ticks}-aimb{alpha_imbalance}-treset{time_reset}'
    output_folder = output_dir / f'{output_file_prefix}'
    output_folder.mkdir(parents=True, exist_ok=True)
    logging.info(f"Pasta de saída criada: {output_folder}")

    # O estado inicial precisa ter 15 elementos, incluindo 'warm'
    system_state = (
        np.nan, -np.inf, np.inf, np.nan, np.nan, np.nan, # bar_open, high, low, close, start_time, end_time (6)
        0.0, 0.0, 0.0, 0.0, # current_imbalance, buy_volume_usd, total_volume_usd, total_volume (4)
        0.0, 0.0,           # ticks, ticks_buy (2)
        float(init_ticks), 0.0, # ewma_T, ewma_imbalance (2) - ewma_imbalance começa em 0.0
        True # warm (1)
    )

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
            file_name = f'bars_part_{i+1:03d}.parquet'
            output_path = output_folder / file_name
            bars.to_parquet(output_path, index=False)
            elapsed_time = (time.time() - start_time_file) / 60
            logging.info(f"Arquivo {file_name} salvo. {len(bars)} barras geradas. Tempo: {elapsed_time:.2f} min.")
        else:
            logging.warning(f"Nenhuma barra gerada para o arquivo {file}")

    client.close()

    # Merge dos arquivos parciais
    all_bars = [pd.read_parquet(pf) for pf in sorted(output_folder.glob('bars_part_*.parquet'))]
    if all_bars:
        df_all_bars = pd.concat(all_bars, ignore_index=True)
        # A coluna 'end_time' já vem em nanosegundos (int64), então convertemos para datetime
        df_all_bars['end_time'] = pd.to_datetime(df_all_bars['end_time'], unit='ns')
        df_all_bars.drop(columns=['start_time'], inplace=True)

        final_path = output_folder / f'{output_file_prefix}.parquet'
        df_all_bars.to_parquet(final_path, index=False)
        logging.info(f"\nProcessamento completo! Arquivo final salvo em: {final_path}")
        logging.info(f"Total de barras no arquivo final: {len(df_all_bars)}")
    else:
        logging.warning("\nNenhum arquivo parcial foi gerado para o merge.")

def main():
    """Função principal para execução via linha de comando."""
    parser = argparse.ArgumentParser(description='Gerador de Imbalance Dollar Bars - Versão Integrada')
    parser.add_argument('--data-type', default='futures', help='Tipo de dados (spot, futures)')
    parser.add_argument('--futures-type', default='um', help='Tipo de futures (um, cm)')
    parser.add_argument('--granularity', default='daily', help='Granularidade (daily, monthly)')
    parser.add_argument('--init_ticks', type=float, default=100, help='Ticks inicial esperado por barra')
    parser.add_argument('--alpha_ticks', type=float, default=0.9, help='Fator de decay para EWMA de ticks')
    parser.add_argument('--alpha_imbalance', type=float, default=0.9, help='Fator de decay para EWMA do imbalance')
    parser.add_argument('--time_reset', type=float, default=8, help='Tempo em horas para resetar os EWMAs se a barra demorar muito')
    parser.add_argument('--output-dir', type=str, help='Diretório de saída personalizado')
    args = parser.parse_args()

    process_files_and_generate_bars(
        data_type=args.data_type, futures_type=args.futures_type, granularity=args.granularity,
        init_ticks=args.init_ticks,
        alpha_ticks=args.alpha_ticks, alpha_imbalance=args.alpha_imbalance,
        output_dir=args.output_dir, time_reset=args.time_reset
    )

if __name__ == '__main__':
    main()