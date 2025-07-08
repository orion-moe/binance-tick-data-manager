#!/usr/bin/env python3
"""
Imbalance Dollar Bars Generator - Versão Corrigida

Este módulo gera "imbalance dollar bars" a partir de dados de trades do Bitcoin.
Usa processamento distribuído com Dask e otimizações com Numba para processar
grandes volumes de dados eficientemente.

CORREÇÕES:
- Redução de workers e memória para evitar crashes
- Tratamento de exceções e limpeza de recursos
- Garbage collection forçado
- Timeout para fechamento do cliente

Baseado no notebook: imbalance_dollar_barsv3_fixedv2.ipynb
"""

import os
import argparse
import datetime
import logging
import time
import gc
import warnings
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client
from numba import njit, types
from numba.typed import List

# Suprime avisos do Dask
warnings.filterwarnings('ignore', category=UserWarning)


def setup_logging():
    """Configura o sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def setup_dask_client(n_workers=10, threads_per_worker=1, memory_limit='2GB'):
    """
    Configura o cliente Dask para processamento distribuído.

    Valores reduzidos para evitar problemas de memória:
    - n_workers: 2 (reduzido de 10)
    - memory_limit: 1GB por worker (reduzido de 6.4GB)
    """
    try:
        client = Client(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            silence_logs=logging.ERROR,
            processes=True  # Usar processos separados
        )
        logging.info(f"Dask client inicializado: {client}")
        return client
    except Exception as e:
        logging.error(f"Erro ao inicializar Dask: {e}")
        # Tenta uma configuração ainda mais conservadora
        client = Client(
            n_workers=1,
            threads_per_worker=1,
            memory_limit='500MB',
            silence_logs=logging.ERROR
        )
        logging.info("Dask inicializado com configuração mínima")
        return client


def get_data_path(data_type='futures', futures_type='um', granularity='daily'):
    """
    Constrói o caminho para os dados baseado nos parâmetros.

    Args:
        data_type: 'spot' ou 'futures'
        futures_type: 'um' ou 'cm' (apenas para futures)
        granularity: 'daily' ou 'monthly'

    Returns:
        Path para os dados
    """
    project_root = Path(__file__).parent

    if data_type == 'spot':
        return project_root / 'datasets' / f'dataset-raw-{granularity}-compressed-optimized' / 'spot'
    else:
        return project_root / 'datasets' / f'dataset-raw-{granularity}-compressed-optimized' / f'futures-{futures_type}'


def read_parquet_files_optimized(raw_dataset_path, file):
    """Lê arquivos Parquet de forma otimizada com chunks menores."""
    parquet_pattern = os.path.join(raw_dataset_path, file)
    df_dask = dd.read_parquet(
        parquet_pattern,
        columns=['price', 'qty', 'quoteQty', 'time'],
        engine='pyarrow',
        dtype={'price': 'float32', 'qty': 'float32', 'quoteQty': 'float32'},
        chunksize='100MB'  # Limita tamanho dos chunks
    )
    return df_dask


def assign_side_optimized(df):
    """Atribui o lado da negociação com base na mudança de preço."""
    df['side'] = np.where(df['price'].shift() > df['price'], 1,
                          np.where(df['price'].shift() < df['price'], -1, np.nan))
    df['side'] = df['side'].ffill().fillna(1).astype('int8')
    return df


def apply_operations_optimized(df_dask, meta):
    """Aplica operações otimizadas no DataFrame."""
    df_dask = df_dask.map_partitions(assign_side_optimized, meta=meta)
    df_dask['dollar_imbalance'] = df_dask['quoteQty'] * df_dask['side']
    return df_dask


# Função compilada com numba
@njit(
    types.Tuple((
        types.ListType(types.Tuple((
            types.float64,  # start_time
            types.float64,  # end_time
            types.float64,  # open
            types.float64,  # high
            types.float64,  # low
            types.float64,  # close
            types.float64,  # imbalance_col
            types.float64,  # total_volume_buy_usd
            types.float64,  # total_volume_usd
            types.float64   # total_volume
        ))),
        types.float64,  # exp_T
        types.float64,  # exp_dif
        types.Tuple((
            types.float64,  # bar_open
            types.float64,  # bar_high
            types.float64,  # bar_low
            types.float64,  # bar_close
            types.float64,  # bar_start_time
            types.float64,  # bar_end_time
            types.float64,  # current_imbalance
            types.float64,  # buy_volume_usd
            types.float64,  # total_volume_usd
            types.float64   # total_volume
        )),
        types.ListType(types.Tuple((
            types.float64,  # exp_T
            types.float64,  # exp_dif
        )))
    ))(
        types.float64[:],  # prices
        types.float64[:],  # times
        types.float64[:],  # imbalances
        types.int8[:],     # sides
        types.float64[:],  # qtys
        types.float64,     # init_T
        types.float64,     # init_dif
        types.float64,     # alpha_volume
        types.float64,     # alpha_imbalance
        types.Tuple((
            types.float64,  # bar_open
            types.float64,  # bar_high
            types.float64,  # bar_low
            types.float64,  # bar_close
            types.float64,  # bar_start_time
            types.float64,  # bar_end_time
            types.float64,  # current_imbalance
            types.float64,  # buy_volume_usd
            types.float64,  # total_volume_usd
            types.float64   # total_volume
        ))
    )
)
def process_partition_imbalance_numba(
    prices, times, imbalances, sides, qtys,
    init_T, init_dif, alpha_volume, alpha_imbalance, res_init
):
    """Processa uma partição usando numba para aceleração."""
    exp_T = init_T
    exp_dif = init_dif
    threshold = exp_T * abs(exp_dif)

    bars = List()
    params = List()

    # Desempacota res_init
    bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
    current_imbalance, buy_volume_usd, total_volume_usd, total_volume = res_init

    # Verifica se res_init está inicializado
    if bar_open == -1.0:
        bar_open = np.nan
        bar_high = -np.inf
        bar_low = np.inf
        bar_close = np.nan
        bar_start_time = np.nan
        bar_end_time = np.nan
        current_imbalance = 0.0
        buy_volume_usd = 0.0
        total_volume_usd = 0.0
        total_volume = 0.0

    for i in range(len(prices)):
        if np.isnan(bar_open):
            bar_open = prices[i]
            bar_start_time = times[i]

        trade_price = prices[i]
        bar_high = max(bar_high, trade_price)
        bar_low = min(bar_low, trade_price)
        bar_close = trade_price

        trade_imbalance = imbalances[i]

        if sides[i] > 0:
            buy_volume_usd += trade_imbalance

        total_volume += qtys[i]
        total_volume_usd += abs(trade_imbalance)
        current_imbalance += trade_imbalance
        imbalance = abs(current_imbalance)

        if imbalance >= threshold:
            bar_end_time = times[i]

            # Salva a barra formada
            bars.append((
                bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                current_imbalance, buy_volume_usd, total_volume_usd, total_volume
            ))

            # Atualiza os valores exponenciais (período warm-up)
            if exp_dif == 1.0:
                exp_T = total_volume_usd
                exp_dif = abs(2 * buy_volume_usd / total_volume_usd - 1)
            else:
                exp_T += alpha_volume * (total_volume_usd - exp_T)
                exp_dif += alpha_imbalance * (abs(2 * buy_volume_usd / total_volume_usd - 1) - exp_dif)

            threshold = exp_T * abs(exp_dif)

            params.append((exp_T, exp_dif))

            # Reseta as variáveis de agregação
            bar_open = np.nan
            bar_high = -np.inf
            bar_low = np.inf
            bar_close = np.nan
            bar_start_time = np.nan
            bar_end_time = np.nan
            current_imbalance = 0.0
            buy_volume_usd = 0.0
            total_volume_usd = 0.0
            total_volume = 0.0

    # Prepara o estado final para a próxima partição
    final_state = (
        bar_open, bar_high, bar_low, bar_close,
        bar_start_time, bar_end_time, current_imbalance,
        buy_volume_usd, total_volume_usd, total_volume
    )

    return bars, exp_T, exp_dif, final_state, params


def create_imbalance_dollar_bars_numba(partition, init_T, init_dif, res_init, alpha_volume, alpha_imbalance):
    """Função wrapper para processar uma partição com numba."""
    # Converte a partição para arrays numpy
    prices = partition['price'].values.astype(np.float64)
    times = partition['time'].values.astype(np.float64)
    imbalances = partition['dollar_imbalance'].values.astype(np.float64)
    sides = partition['side'].values.astype(np.int8)
    qtys = partition['qty'].values.astype(np.float64)

    # Inicializa res_init se vazio ou inválido
    if res_init is None or len(res_init) != 10:
        res_init = (-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, 0.0, 0.0, 0.0, 0.0)

    # Processa a partição usando a função compilada com numba
    bars, exp_T, exp_dif, res_init, params = process_partition_imbalance_numba(
        prices, times, imbalances, sides, qtys,
        init_T, init_dif, alpha_volume, alpha_imbalance, res_init
    )

    # Converte as barras para um DataFrame
    if len(bars) > 0:
        bars_df = pd.DataFrame(bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
        params_df = pd.DataFrame(params, columns=['ewma_volume', 'ewma_dif'])
    else:
        bars_df = pd.DataFrame(columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
        params_df = pd.DataFrame(columns=['ewma_volume', 'ewma_dif'])

    return bars_df, exp_T, exp_dif, res_init, params_df


def batch_create_imbalance_dollar_bars_optimized(df_dask, init_T, init_dif, res_init, alpha_volume, alpha_imbalance):
    """Processa partições em lote para criar barras de desequilíbrio em dólares."""
    results = []
    params_save = []

    # Limpa memória antes de processar
    gc.collect()

    for partition in range(df_dask.npartitions):
        logging.info(f'Processando partição {partition+1} de {df_dask.npartitions}')

        try:
            part = df_dask.get_partition(partition).compute()

            bars, init_T, init_dif, res_init, params = create_imbalance_dollar_bars_numba(
                part, init_T, init_dif, res_init, alpha_volume, alpha_imbalance
            )
            results.append(bars)
            params_save.append(params)

            # Libera memória da partição
            del part

            # Garbage collection periodicamente
            if partition % 10 == 0:
                gc.collect()

        except Exception as e:
            logging.error(f"Erro ao processar partição {partition}: {e}")
            continue

    # Filtra DataFrames vazios
    results = [df for df in results if not df.empty]
    params_save = [df for df in params_save if not df.empty]

    if results:
        results_df = pd.concat(results, ignore_index=True)
        params_df = pd.concat(params_save, ignore_index=True)
    else:
        results_df = pd.DataFrame(columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
        params_df = pd.DataFrame(columns=['ewma_volume', 'ewma_dif'])

    return results_df, init_T, init_dif, res_init, params_df


def process_imbalance_dollar_bars(
    data_type='futures',
    futures_type='um',
    granularity='daily',
    init_T0=1000000,
    alpha_volume=0.1,
    alpha_imbalance=0.1,
    output_dir=None,
    n_workers=2,
    memory_limit='1GB'
):
    """
    Função principal para processar imbalance dollar bars.

    Args:
        data_type: 'spot' ou 'futures'
        futures_type: 'um' ou 'cm' (apenas para futures)
        granularity: 'daily' ou 'monthly'
        init_T0: Threshold inicial
        alpha_volume: Fator de decay para volume
        alpha_imbalance: Fator de decay para imbalance
        output_dir: Diretório de saída (opcional)
        n_workers: Número de workers Dask
        memory_limit: Limite de memória por worker
    """
    # Configuração inicial
    setup_logging()
    client = None

    try:
        client = setup_dask_client(n_workers=n_workers, memory_limit=memory_limit)

        # Caminhos
        raw_dataset_path = get_data_path(data_type, futures_type, granularity)

        if output_dir is None:
            output_dir = Path(__file__).parent / 'output'
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(exist_ok=True)

        logging.info(f"Caminho dos dados: {raw_dataset_path}")
        logging.info(f"Diretório de saída: {output_dir}")

        # Verifica se o diretório existe
        if not raw_dataset_path.exists():
            logging.error(f"Diretório de dados não encontrado: {raw_dataset_path}")
            return

        # Lista arquivos
        files = sorted([f for f in os.listdir(raw_dataset_path) if f.endswith('.parquet')])
        file_count = len(files)

        if file_count == 0:
            logging.error("Nenhum arquivo Parquet encontrado!")
            return

        logging.info(f"Encontrados {file_count} arquivos Parquet")

        # Meta DataFrame para map_partitions
        meta = pd.DataFrame({
            'price': pd.Series(dtype='float32'),
            'qty': pd.Series(dtype='float32'),
            'quoteQty': pd.Series(dtype='float32'),
            'time': pd.Series(dtype='float64'),
            'side': pd.Series(dtype='int8')
        })

        # Processamento
        start_time = time.time()
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        output_file = f'imbalance_dollar_{init_T0}-{alpha_volume}-{alpha_imbalance}'

        results = pd.DataFrame()
        params = pd.DataFrame()
        init_dif = 1.0
        res_init = (-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, 0.0, 0.0, 0.0, 0.0)
        init_T = init_T0

        logging.info(f"Iniciando processamento: {output_file}")

        for number, file in enumerate(files, 1):
            logging.info(f"Processando arquivo {number} de {file_count}: {file}")

            file_path = raw_dataset_path / file
            if not file_path.exists():
                logging.warning(f"Arquivo não encontrado: {file}")
                continue

            try:
                df_dask = read_parquet_files_optimized(str(raw_dataset_path), file)
                df_dask = apply_operations_optimized(df_dask, meta)

                bars, init_T, init_dif, res_init, params_df = batch_create_imbalance_dollar_bars_optimized(
                    df_dask, init_T, init_dif, res_init, alpha_volume, alpha_imbalance
                )

                results = pd.concat([results, bars], ignore_index=True)
                params = pd.concat([params, params_df], ignore_index=True)

                # Limpa memória após cada arquivo
                del df_dask
                gc.collect()

            except Exception as e:
                logging.error(f"Erro ao processar {file}: {e}")
                continue

            # Processa última barra se for o último arquivo
            if number == file_count:
                bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
                current_imbalance, buy_volume_usd, total_volume_usd, total_volume = res_init

                if not np.isnan(bar_open):
                    # Lê apenas o último valor do arquivo para obter o timestamp final
                    try:
                        last_time = pd.read_parquet(file_path, columns=['time']).iloc[-1]['time']
                        bar_end_time = last_time

                        lastbar = pd.DataFrame([[
                            bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                            current_imbalance, buy_volume_usd, total_volume_usd, total_volume
                        ]], columns=[
                            'start_time', 'end_time', 'open', 'high', 'low', 'close',
                            'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
                        ])

                        results = pd.concat([results, lastbar], ignore_index=True)
                    except Exception as e:
                        logging.warning(f"Erro ao processar última barra: {e}")

        # Finaliza processamento
        if not results.empty:
            results_final = results.copy()
            results_final['start_time'] = pd.to_datetime(results_final['start_time'])
            results_final['end_time'] = pd.to_datetime(results_final['end_time'])
            results_final.drop(columns=['start_time'], inplace=True)

            # Salva arquivo
            output_path = output_dir / f'{output_file}_{timestamp}.parquet'
            results_final.to_parquet(output_path, index=False)

            # Salva parâmetros se houver
            if not params.empty:
                params_path = output_dir / f'params_{output_file}_{timestamp}.parquet'
                params.to_parquet(params_path, index=False)

            end_time = time.time()
            elapsed_time = (end_time - start_time) / 60

            logging.info(f"Processamento concluído em {elapsed_time:.2f} minutos")
            logging.info(f"Arquivo salvo: {output_path}")
            logging.info(f"Total de barras geradas: {len(results_final)}")
        else:
            logging.warning("Nenhuma barra foi gerada!")

    except Exception as e:
        logging.error(f"Erro geral no processamento: {e}")

    finally:
        # Fecha cliente Dask e limpa recursos
        if client:
            try:
                client.close(timeout=10)
            except Exception as e:
                logging.warning(f"Erro ao fechar cliente Dask: {e}")

            try:
                client.shutdown()
            except:
                pass

        # Força garbage collection final
        gc.collect()


def main():
    """Função principal para execução via linha de comando."""
    parser = argparse.ArgumentParser(description='Gerador de Imbalance Dollar Bars - Versão Corrigida')

    parser.add_argument('--data-type', choices=['spot', 'futures'], default='futures',
                        help='Tipo de dados (padrão: futures)')
    parser.add_argument('--futures-type', choices=['um', 'cm'], default='um',
                        help='Tipo de futures (padrão: um)')
    parser.add_argument('--granularity', choices=['daily', 'monthly'], default='daily',
                        help='Granularidade (padrão: daily)')
    parser.add_argument('--init-T', type=float, default=1000000,
                        help='Threshold inicial (padrão: 1000000)')
    parser.add_argument('--alpha-volume', type=float, default=0.1,
                        help='Fator de decay para volume (padrão: 0.1)')
    parser.add_argument('--alpha-imbalance', type=float, default=0.1,
                        help='Fator de decay para imbalance (padrão: 0.1)')
    parser.add_argument('--output-dir', type=str,
                        help='Diretório de saída (padrão: ./output)')
    parser.add_argument('--n-workers', type=int, default=2,
                        help='Número de workers Dask (padrão: 2)')
    parser.add_argument('--memory-limit', type=str, default='1GB',
                        help='Limite de memória por worker (padrão: 1GB)')

    args = parser.parse_args()

    process_imbalance_dollar_bars(
        data_type=args.data_type,
        futures_type=args.futures_type,
        granularity=args.granularity,
        init_T0=args.init_T,
        alpha_volume=args.alpha_volume,
        alpha_imbalance=args.alpha_imbalance,
        output_dir=args.output_dir,
        n_workers=args.n_workers,
        memory_limit=args.memory_limit
    )


if __name__ == '__main__':
    main()