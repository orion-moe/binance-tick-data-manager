#!/usr/bin/env python3
"""
Imbalance Dollar Bars Generator - Versão Modificada

Este módulo gera "imbalance dollar bars" a partir de dados de trades do Bitcoin.
Usa processamento distribuído com Dask e otimizações com Numba para processar
grandes volumes de dados eficientemente.

MODIFICAÇÕES:
- Reset no threshold quando a barra demorar mais de 1 hora para ser formada
- Gatilho baseado em volume total (init_T0) em vez de imbalance

Baseado no notebook: imbalance_dollar_barsv3_fixedv2.ipynb
"""

import os
import argparse
import datetime
import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client
from numba import njit, types
from numba.typed import List


def setup_logging():
    """Configura o sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def setup_dask_client(n_workers=10, threads_per_worker=1, memory_limit='6.4GB'):
    """Configura o cliente Dask para processamento distribuído."""
    client = Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit
    )
    logging.info(f"Dask client inicializado: {client}")
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
    project_root = Path(__file__).parent.parent.parent  # Sobe dois níveis para a raiz do projeto

    if data_type == 'spot':
        return project_root / 'data' / f'dataset-raw-{granularity}-compressed-optimized' / 'spot'
    else:
        return project_root / 'data' / f'dataset-raw-{granularity}-compressed-optimized' / f'futures-{futures_type}'


def read_parquet_files_optimized(raw_dataset_path, file):
    """Lê arquivos Parquet de forma otimizada."""
    parquet_pattern = os.path.join(raw_dataset_path, file)
    df_dask = dd.read_parquet(
        parquet_pattern,
        columns=['price', 'qty', 'quoteQty', 'time'],
        engine='pyarrow',
        dtype={'price': 'float32', 'qty': 'float32', 'quoteQty': 'float32'}
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
    df_dask['volumes'] = df_dask['quoteQty'] * df_dask['side']
    return df_dask


@njit(
    types.Tuple((
        types.ListType(types.Tuple((
            types.float64,  # start_time
            types.float64,  # end_time
            types.float64,  # open
            types.float64,  # high
            types.float64,  # low
            types.float64,  # close
            types.float64,  # theta_k
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
        )),
        types.float64,      # init_T0
        types.float64       # Reset time
    )
)
def process_partition_run_numba(
    prices, times, volumes, sides, qtys,
    init_T, init_dif, alpha_volume, alpha_imbalance, res_init,
    init_T0, reset_time
):
    """
    Processa uma partição usando numba para aceleração.

    """

    # MODIFICAÇÃO: Usar init_T diretamente como threshold inicial
    bars = List()
    params = List()

    # Desempacota res_init
    bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
        current_imbalance, buy_volume_usd, total_volume_usd, total_volume = res_init

    # Inicializa EWMA values
    ewma_T = init_T
    ewma_imbalance = init_dif

    threshold = ewma_T * ewma_imbalance

    # Check se res_init está inicializado
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
        ticks = 0.0
    # mark = 0
    for i in range(len(prices)):
        # print(mark)
        # mark += 1
        if np.isnan(bar_open):
            bar_open = prices[i]
            bar_start_time = times[i]

        trade_price = prices[i]
        bar_high = max(bar_high, trade_price)
        bar_low = min(bar_low, trade_price)
        bar_close = trade_price

        trade_imbalance = volumes[i]

        if sides[i] > 0:
            buy_volume_usd += trade_imbalance

        total_volume += qtys[i]
        total_volume_usd += abs(trade_imbalance)
        current_imbalance += trade_imbalance

        # time_since_bar_start = times[i] - bar_start_time

        var_buy = buy_volume_usd
        v_sell = buy_volume_usd - total_volume_usd
        var = max(var_buy, -v_sell)

        # Gatilho baseado em volume total
        if var >= threshold:
            bar_end_time = times[i]

            # Save a barra formada
            bars.append((
                bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                current_imbalance, buy_volume_usd, total_volume_usd, total_volume
                ))
            if ewma_imbalance == 1.0:
                ewma_T = total_volume_usd
                ewma_imbalance = abs(2 * buy_volume_usd / total_volume_usd - 1)
                threshold = ewma_T * ewma_imbalance
            # elif time_since_bar_start > ONE_HOUR_MS:
            #     ewma_T = init_T0
            #     ewma_imbalance += alpha_imbalance * (abs(2 * buy_volume_usd / total_volume_usd - 1) - ewma_imbalance)
            #     threshold = ewma_T
            else:
                ewma_T += alpha_volume * (total_volume_usd - ewma_T)
                ewma_imbalance += alpha_imbalance * (abs(2 * buy_volume_usd / total_volume_usd - 1) - ewma_imbalance)
                threshold = ewma_T * ewma_imbalance

            params.append((ewma_T, ewma_imbalance))

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

    return bars, ewma_T, ewma_imbalance, final_state, params


def create_run_dollar_bars_numba(partition, init_T, init_dif, res_init, alpha_volume, alpha_imbalance, init_T0, reset_time):
    """Função wrapper para processar uma partição com numba."""
    # Convert a partição para arrays numpy
    prices = partition['price'].values.astype(np.float64)
    times = partition['time'].values.astype(np.float64)
    imbalances = partition['dollar_imbalance'].values.astype(np.float64)
    sides = partition['side'].values.astype(np.int8)
    qtys = partition['qty'].values.astype(np.float64)

    # Inicializa res_init se vazio ou inválido
    if res_init is None or len(res_init) != 10:
        res_init = (-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, 0.0, 0.0, 0.0, 0.0)

    # Process a partição usando a função compilada com numba
    bars, ewma_T, ewma_imbalance, res_init, params = process_partition_run_numba(
        prices, times, imbalances, sides, qtys,
        init_T, init_dif, alpha_volume, alpha_imbalance, res_init,
        init_T0, reset_time
    )

    # Convert as barras para um DataFrame
    if len(bars) > 0:
        bars_df = pd.DataFrame(bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'theta_k', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
        params_df = pd.DataFrame(params, columns=['ewma_volume', 'ewma_dif'])
    else:
        bars_df = pd.DataFrame(columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'theta_k', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
        params_df = pd.DataFrame(columns=['ewma_volume', 'ewma_dif'])

    return bars_df, ewma_T, ewma_imbalance, res_init, params_df


def batch_create_run_dollar_bars_optimized(df_dask, init_T, init_dif, res_init, alpha_volume, alpha_imbalance, init_T0, reset_time):
    """Processa partições em lote para criar barras de desequilíbrio em dólares."""
    results = []
    params_save = []

    for partition in range(df_dask.npartitions):
        logging.info(f'Processing partition {partition+1} de {df_dask.npartitions}')
        part = df_dask.get_partition(partition).compute()

        bars, init_T, init_dif, res_init, params = create_run_dollar_bars_numba(
            part, init_T, init_dif, res_init, alpha_volume, alpha_imbalance, init_T0, reset_time
        )

        results.append(bars)
        params_save.append(params)

    # Filtra DataFrames vazios
    results = [df for df in results if not df.empty]
    params_save = [df for df in params_save if not df.empty]
    if results:
        results_df = pd.concat(results, ignore_index=True)
        params_df = pd.concat(params_save, ignore_index=True)
    else:
        results_df = pd.DataFrame(columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'theta_k', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
        params_df = pd.DataFrame(columns=['ewma_volume', 'ewma_dif'])

    return results_df, init_T, init_dif, res_init, params_df


def merge_parquet_files(output_dir, pattern, final_filename):
    """
    Faz o merge de todos os arquivos parquet que correspondem ao padrão.

    Args:
        output_dir: Diretório onde estão os arquivos
        pattern: Padrão para buscar os arquivos (ex: 'imbalance_dollar_volume_*')
        final_filename: Nome do arquivo final após o merge
    """
    import glob

    # Busca todos os diretórios que correspondem ao padrão
    subdirs = sorted(glob.glob(os.path.join(output_dir, f"{pattern}*")))

    if not subdirs:
        logging.warning(f"Nenhum diretório encontrado com o padrão: {pattern}")
        return None

    logging.info(f"Encontrados {len(subdirs)} diretórios para merge")

    # Lista para armazenar os DataFrames
    all_bars = []

    # Read cada arquivo parquet
    for subdir in subdirs:
        parquet_files = glob.glob(os.path.join(subdir, "*.parquet"))
        if parquet_files:
            parquet_file = parquet_files[0]  # Assume um arquivo por diretório
            logging.info(f"Lendo: {parquet_file}")
            df = pd.read_parquet(parquet_file)
            all_bars.append(df)

    if not all_bars:
        logging.warning("Nenhum arquivo parquet encontrado para merge")
        return None

    # Concatena todos os DataFrames
    merged_df = pd.concat(all_bars, ignore_index=True)

    # Ordena por end_time
    merged_df = merged_df.sort_values('end_time').reset_index(drop=True)

    # Save o arquivo final
    final_path = os.path.join(output_dir, f"{final_filename}.parquet")
    merged_df.to_parquet(final_path, index=False)

    logging.info(f"Arquivo final salvo: {final_path}")
    logging.info(f"Total de barras no arquivo final: {len(merged_df)}")

    return final_path


def process_run_dollar_bars(
    data_type='futures',
    futures_type='um',
    granularity='daily',
    init_T0=10_000_000,
    alpha_volume=0.9,
    alpha_imbalance=0.9,
    output_dir=None,
    time_reset=4
    ):
    """
    Função principal para processar imbalance dollar bars.

    Args:
        data_type: 'spot' ou 'futures'
        futures_type: 'um' ou 'cm' (apenas para futures)
        granularity: 'daily' ou 'monthly'
        init_T0: Threshold inicial (agora usado diretamente para volume)
        alpha_volume: Fator de decay para volume
        alpha_imbalance: Fator de decay para imbalance
        output_dir: Diretório de saída (opcional)
    """
    # Configuration inicial
    setup_logging()
    client = setup_dask_client()

    # Paths
    raw_dataset_path = get_data_path(data_type, futures_type, granularity)

    if output_dir is None:
        output_dir = Path(__file__).parent / 'output'
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(exist_ok=True)

    logging.info(f"Caminho dos dados: {raw_dataset_path}")
    logging.info(f"Diretório de saída: {output_dir}")
    logging.info(f"MODIFICAÇÕES ATIVAS:")
    logging.info(f"  - Gatilho baseado em volume total (threshold = {init_T0})")
    logging.info(f"  - Reset de threshold após 1 hora sem formar barra")

    # Check se o diretório existe
    if not raw_dataset_path.exists():
        logging.error(f"Diretório de dados não encontrado: {raw_dataset_path}")
        return

    # Lista arquivos
    files = [f for f in os.listdir(raw_dataset_path) if f.endswith('.parquet')]
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

    # Processmento
    start_time = time.time()
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_file = f'run_dollar_volume_{init_T0}-avol{alpha_volume}-aimb{alpha_imbalance}-t{time_reset}'

    init_dif = 1.0
    res_init = (-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, 0.0, 0.0, 0.0, 0.0)
    init_T = init_T0
    time_reset = 4
    logging.info(f"Iniciando processamento: {output_file}")

    # Cria pasta única para todos os arquivos
    output_folder = output_dir / f'{output_file}_{timestamp}'
    output_folder.mkdir(parents=True, exist_ok=True)
    logging.info(f"Pasta de saída criada: {output_folder}")

    for number in range(1, file_count + 1):
        number_ = str(number).zfill(3)
        file = f'BTCUSDT-Trades-Optimized-{number_}.parquet'

        logging.info(f"Processando arquivo {number} de {file_count}: {file}")

        file_path = raw_dataset_path / file
        if not file_path.exists():
            logging.warning(f"File not found: {file}")
            continue

        df_dask = read_parquet_files_optimized(str(raw_dataset_path), file)
        df_dask = apply_operations_optimized(df_dask, meta)

        bars, init_T, init_dif, res_init, params_df = batch_create_run_dollar_bars_optimized(
            df_dask, init_T, init_dif, res_init, alpha_volume, alpha_imbalance, init_T0, time_reset
        )

        # Process última barra se for o último arquivo
        if number == file_count:
            bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
            current_imbalance, buy_volume_usd, total_volume_usd, total_volume = res_init

            if not np.isnan(bar_open):
                bar_end_time = df_dask['time'].tail().iloc[-1]

                lastbar = pd.DataFrame([[
                    bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                    current_imbalance, buy_volume_usd, total_volume_usd, total_volume
                ]], columns=[
                    'start_time', 'end_time', 'open', 'high', 'low', 'close',
                    'theta_k', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
                ])

                bars = pd.concat([bars, lastbar], ignore_index=True)

        # Finaliza processamento
        if not bars.empty:
            bars['start_time'] = pd.to_datetime(bars['start_time'])
            bars['end_time'] = pd.to_datetime(bars['end_time'])
            bars.drop(columns=['start_time'], inplace=True)

            print(bars[['end_time', 'close', 'theta_k', 'total_volume_usd']].head(5))

            print(params_df.head(5))
            # break
            bars = pd.concat([bars, params_df], axis=1)


            # Save arquivo único com todos os dados
            file_name = f'bars_part_{number:03d}.parquet'
            output_path = output_folder / file_name
            bars.to_parquet(output_path, index=False)

            end_time = time.time()
            elapsed_time = (end_time - start_time) / 60
            time_between_bars = bars['end_time'].diff().dt.total_seconds() / 60
            time_between_bars_mean = round(time_between_bars.mean(), 2)
            logging.info(f"Processamento concluído em {elapsed_time:.2f} minutos")
            logging.info(f"Arquivo salvo: {output_path}")
            logging.info(f"Total de bars generated original: {len(df_dask)}")
            logging.info(f"Total de bars generated downsampling: {len(bars)}")
            logging.info(f"Redução percentual %: {round(len(bars)/len(df_dask) * 100) - 100}")
            logging.info(f"Tempo médio amostragem: {time_between_bars_mean}")
            logging.info(f"Colunas no arquivo: {list(bars.columns)}")
        else:
            end_time = time.time()
            elapsed_time = (end_time - start_time) / 60
            logging.warning("No bars were generated!")
            logging.info(f"Processamento concluído em {elapsed_time:.2f} minutos")

    # Fecha cliente Dask
    client.close()

    # Faz o merge de todos os arquivos gerados
    logging.info("\nIniciando merge de todos os arquivos gerados...")

    # Lista todos os arquivos parquet na pasta
    parquet_files = sorted(list(output_folder.glob('bars_part_*.parquet')))

    if parquet_files:
        logging.info(f"Encontrados {len(parquet_files)} arquivos para merge")

        # # Read e concatena todos os arquivos
        # all_bars = []
        # for parquet_file in parquet_files:
        #     logging.info(f"Lendo: {parquet_file}")
        #     df = pd.read_parquet(parquet_file)
        #     all_bars.append(df)

        # # Concatena todos os DataFrames
        # merged_df = pd.concat(all_bars, ignore_index=True)

        merged_df = pd.concat(
            (pd.read_parquet(f) for f in parquet_files),
            ignore_index=True
            )

        # Ordena por end_time
        # merged_df = merged_df.sort_values('end_time').reset_index(drop=True)

        # Save o arquivo final na mesma pasta com o nome raiz
        final_filename = f'{output_file}_{timestamp}.parquet'
        final_path = output_folder / final_filename
        merged_df.to_parquet(final_path, index=False)

        logging.info(f"\nProcessamento completo! Arquivo final: {final_path}")
        logging.info(f"Total de barras no arquivo final: {len(merged_df)}")
    else:
        logging.warning("\nNão foi possível fazer o merge dos arquivos")


def main():
    """Função principal para execução via linha de comando."""
    parser = argparse.ArgumentParser(description='Gerador de Imbalance Dollar Bars - Versão Modificada')

    parser.add_argument('--data-type', choices=['spot', 'futures'], default='spot',
                        help='Tipo de dados (padrão: futures)')
    parser.add_argument('--futures-type', choices=['um', 'cm'], default='um',
                        help='Tipo de futures (padrão: um)')
    parser.add_argument('--granularity', choices=['daily', 'monthly'], default='daily',
                        help='Granularidade (padrão: daily)')
    parser.add_argument('--init-T', type=float, default=1_000_000_000,
                        help='Threshold inicial para volume (padrão: 10000000)')
    parser.add_argument('--alpha-volume', type=float, default=0.1,
                        help='Fator de decay para volume (padrão: 0.5)')
    parser.add_argument('--alpha-imbalance', type=float, default=0.1,
                        help='Fator de decay para imbalance (padrão: 0.5)')
    parser.add_argument('--output-dir', type=str,
                        help='Diretório de saída (padrão: ./output)')
    parser.add_argument('--time-reset', type=float, default=4,
                    help='Fator de reset no tempo para imbalance (padrão: 4h)')

    args = parser.parse_args()

    print("=== RUN DOLLAR BARS  ===")
    print("MODIFICAÇÕES:")
    print("- Reset de threshold quando barra demora mais de x hora")
    print("=" * 50)
    print(f"Parâmetros em uso:")
    print(f"  init_T0: {args.init_T:,.0f}")
    print(f"  alpha_volume: {args.alpha_volume}")
    print(f"  alpha_imbalance: {args.alpha_imbalance}")
    print("=" * 50)

    process_run_dollar_bars(
        data_type=args.data_type,
        futures_type=args.futures_type,
        granularity=args.granularity,
        init_T0=args.init_T,
        alpha_volume=args.alpha_volume,
        alpha_imbalance=args.alpha_imbalance,
        output_dir=args.output_dir,
        time_reset=args.time_reset
    )


if __name__ == '__main__':
    main()