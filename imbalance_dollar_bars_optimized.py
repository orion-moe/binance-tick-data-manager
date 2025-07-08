#!/usr/bin/env python3
"""
Imbalance Dollar Bars Generator - Versão Otimizada para Memória

Este módulo gera "imbalance dollar bars" a partir de dados de trades do Bitcoin.
Otimizado para evitar problemas de memória e processos killed.

OTIMIZAÇÕES:
- Processamento sequencial sem Dask
- Leitura por chunks
- Liberação agressiva de memória
- Sem multiprocessing para evitar semáforos perdidos

Baseado no notebook: imbalance_dollar_barsv3_fixedv2.ipynb
"""

import os
import argparse
import datetime
import logging
import time
import gc
from pathlib import Path

import numpy as np
import pandas as pd
from numba import njit, types
from numba.typed import List


def setup_logging():
    """Configura o sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


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


def read_parquet_file_chunks(file_path, chunk_size=1000000):
    """
    Lê arquivo Parquet em chunks para economizar memória.
    
    Args:
        file_path: Caminho do arquivo
        chunk_size: Número de linhas por chunk
    
    Yields:
        DataFrame chunks
    """
    # Primeiro, obtém o número total de linhas
    df_meta = pd.read_parquet(file_path, columns=['price'])
    total_rows = len(df_meta)
    del df_meta
    gc.collect()
    
    # Lê em chunks
    for start_row in range(0, total_rows, chunk_size):
        end_row = min(start_row + chunk_size, total_rows)
        
        # Lê apenas as colunas necessárias
        chunk = pd.read_parquet(
            file_path,
            columns=['price', 'qty', 'quoteQty', 'time'],
            engine='pyarrow'
        ).iloc[start_row:end_row]
        
        # Converte tipos para economizar memória
        chunk['price'] = chunk['price'].astype('float32')
        chunk['qty'] = chunk['qty'].astype('float32')
        chunk['quoteQty'] = chunk['quoteQty'].astype('float32')
        
        yield chunk


def assign_side_optimized(df):
    """Atribui o lado da negociação com base na mudança de preço."""
    df['side'] = np.where(df['price'].shift() > df['price'], 1,
                          np.where(df['price'].shift() < df['price'], -1, np.nan))
    df['side'] = df['side'].ffill().fillna(1).astype('int8')
    df['dollar_imbalance'] = df['quoteQty'] * df['side']
    return df


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
        )),
        types.float64      # init_T0
    )
)
def process_partition_imbalance_numba(
    prices, times, imbalances, sides, qtys,
    init_T, init_dif, alpha_volume, alpha_imbalance, res_init, init_T0
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


def process_file_sequential(
    file_path, 
    init_T, 
    init_dif, 
    res_init, 
    alpha_volume, 
    alpha_imbalance, 
    init_T0,
    chunk_size=1000000
):
    """
    Processa um arquivo sequencialmente por chunks.
    
    Args:
        file_path: Caminho do arquivo
        init_T: Threshold inicial
        init_dif: Diferença inicial
        res_init: Estado inicial
        alpha_volume: Fator de decay para volume
        alpha_imbalance: Fator de decay para imbalance
        init_T0: Threshold inicial para reset
        chunk_size: Tamanho do chunk
    
    Returns:
        Tupla com (bars_df, exp_T, exp_dif, res_init, params_df)
    """
    all_bars = []
    all_params = []
    
    chunk_count = 0
    for chunk in read_parquet_file_chunks(file_path, chunk_size):
        chunk_count += 1
        logging.info(f"  Processando chunk {chunk_count}")
        
        # Aplica operações
        chunk = assign_side_optimized(chunk)
        
        # Converte para arrays numpy
        prices = chunk['price'].values.astype(np.float64)
        times = chunk['time'].values.astype(np.float64)
        imbalances = chunk['dollar_imbalance'].values.astype(np.float64)
        sides = chunk['side'].values.astype(np.int8)
        qtys = chunk['qty'].values.astype(np.float64)
        
        # Libera o chunk DataFrame
        del chunk
        gc.collect()
        
        # Processa com numba
        bars, init_T, init_dif, res_init, params = process_partition_imbalance_numba(
            prices, times, imbalances, sides, qtys,
            init_T, init_dif, alpha_volume, alpha_imbalance, res_init, init_T0
        )
        
        # Adiciona resultados
        if len(bars) > 0:
            all_bars.extend(bars)
        if len(params) > 0:
            all_params.extend(params)
        
        # Libera arrays
        del prices, times, imbalances, sides, qtys
        gc.collect()
    
    # Converte para DataFrames
    if all_bars:
        bars_df = pd.DataFrame(all_bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
    else:
        bars_df = pd.DataFrame(columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
        ])
    
    if all_params:
        params_df = pd.DataFrame(all_params, columns=['ewma_volume', 'ewma_dif'])
    else:
        params_df = pd.DataFrame(columns=['ewma_volume', 'ewma_dif'])
    
    return bars_df, init_T, init_dif, res_init, params_df


def process_imbalance_dollar_bars(
    data_type='futures',
    futures_type='um', 
    granularity='daily',
    init_T0=10000000,
    alpha_volume=0.5,
    alpha_imbalance=0.5,
    output_dir=None,
    chunk_size=1000000
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
        chunk_size: Tamanho do chunk para leitura
    """
    # Configuração inicial
    setup_logging()
    
    # Força coleta de lixo inicial
    gc.collect()
    
    # Caminhos
    raw_dataset_path = get_data_path(data_type, futures_type, granularity)
    
    if output_dir is None:
        output_dir = Path(__file__).parent / 'output'
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(exist_ok=True)
    
    logging.info(f"Caminho dos dados: {raw_dataset_path}")
    logging.info(f"Diretório de saída: {output_dir}")
    logging.info(f"Processamento sequencial com chunks de {chunk_size:,} linhas")
    
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
    
    # Processamento
    start_time = time.time()
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_file = f'imbalance_dollar_{init_T0}-{alpha_volume}-{alpha_imbalance}'
    
    results = []
    params = []
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
            # Processa arquivo sequencialmente
            bars_df, init_T, init_dif, res_init, params_df = process_file_sequential(
                file_path, init_T, init_dif, res_init, 
                alpha_volume, alpha_imbalance, init_T0, chunk_size
            )
            
            # Adiciona resultados
            if not bars_df.empty:
                results.append(bars_df)
            if not params_df.empty:
                params.append(params_df)
            
            # Libera memória
            del bars_df, params_df
            gc.collect()
            
        except Exception as e:
            logging.error(f"Erro ao processar {file}: {e}")
            continue
        
        # Processa última barra se for o último arquivo
        if number == file_count and res_init is not None:
            bar_open, bar_high, bar_low, bar_close, bar_start_time, bar_end_time, \
            current_imbalance, buy_volume_usd, total_volume_usd, total_volume = res_init
            
            if not np.isnan(bar_open):
                # Lê apenas o último timestamp
                try:
                    last_df = pd.read_parquet(file_path, columns=['time'])
                    bar_end_time = last_df.iloc[-1]['time']
                    del last_df
                    
                    lastbar = pd.DataFrame([[
                        bar_start_time, bar_end_time, bar_open, bar_high, bar_low, bar_close,
                        current_imbalance, buy_volume_usd, total_volume_usd, total_volume
                    ]], columns=[
                        'start_time', 'end_time', 'open', 'high', 'low', 'close',
                        'imbalance_col', 'total_volume_buy_usd', 'total_volume_usd', 'total_volume'
                    ])
                    
                    results.append(lastbar)
                except Exception as e:
                    logging.warning(f"Erro ao processar última barra: {e}")
    
    # Finaliza processamento
    if results:
        logging.info("Consolidando resultados...")
        results_df = pd.concat(results, ignore_index=True)
        
        results_final = results_df.copy()
        results_final['start_time'] = pd.to_datetime(results_final['start_time'])
        results_final['end_time'] = pd.to_datetime(results_final['end_time'])
        results_final.drop(columns=['start_time'], inplace=True)
        
        # Salva arquivo
        output_path = output_dir / f'{output_file}_{timestamp}.parquet'
        results_final.to_parquet(output_path, index=False)
        
        # Salva parâmetros se houver
        if params:
            params_df = pd.concat(params, ignore_index=True)
            params_path = output_dir / f'params_{output_file}_{timestamp}.parquet'
            params_df.to_parquet(params_path, index=False)
        
        end_time = time.time()
        elapsed_time = (end_time - start_time) / 60
        
        logging.info(f"Processamento concluído em {elapsed_time:.2f} minutos")
        logging.info(f"Arquivo salvo: {output_path}")
        logging.info(f"Total de barras geradas: {len(results_final)}")
    else:
        logging.warning("Nenhuma barra foi gerada!")
    
    # Coleta final de lixo
    gc.collect()


def main():
    """Função principal para execução via linha de comando."""
    parser = argparse.ArgumentParser(
        description='Gerador de Imbalance Dollar Bars - Versão Otimizada'
    )
    
    parser.add_argument('--data-type', choices=['spot', 'futures'], default='futures',
                        help='Tipo de dados (padrão: futures)')
    parser.add_argument('--futures-type', choices=['um', 'cm'], default='um',
                        help='Tipo de futures (padrão: um)')
    parser.add_argument('--granularity', choices=['daily', 'monthly'], default='daily',
                        help='Granularidade (padrão: daily)')
    parser.add_argument('--init-T', type=float, default=10000000,
                        help='Threshold inicial (padrão: 10000000)')
    parser.add_argument('--alpha-volume', type=float, default=0.5,
                        help='Fator de decay para volume (padrão: 0.5)')
    parser.add_argument('--alpha-imbalance', type=float, default=0.5,
                        help='Fator de decay para imbalance (padrão: 0.5)')
    parser.add_argument('--output-dir', type=str,
                        help='Diretório de saída (padrão: ./output)')
    parser.add_argument('--chunk-size', type=int, default=1000000,
                        help='Tamanho do chunk para leitura (padrão: 1000000)')
    
    args = parser.parse_args()
    
    print("=== IMBALANCE DOLLAR BARS - VERSÃO OTIMIZADA ===")
    print("Características:")
    print("- Processamento sequencial (sem Dask)")
    print("- Leitura por chunks")
    print("- Gerenciamento agressivo de memória")
    print("=" * 50)
    
    process_imbalance_dollar_bars(
        data_type=args.data_type,
        futures_type=args.futures_type,
        granularity=args.granularity,
        init_T0=args.init_T,
        alpha_volume=args.alpha_volume,
        alpha_imbalance=args.alpha_imbalance,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size
    )


if __name__ == '__main__':
    main()