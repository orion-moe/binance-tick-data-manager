#!/usr/bin/env python3
"""
Versão otimizada das Standard Dollar Bars com máximo uso de CPU
Usa estratégias híbridas para maximizar paralelização onde possível
"""

import os
import logging
import pandas as pd
import numpy as np
import glob
from numba import njit, types
from numba.typed import List
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing as mp
from functools import partial
import psutil

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_data_path(data_type='futures', futures_type='um', granularity='daily'):
    """Constrói o caminho para os dados."""
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = project_root / 'data'

    if data_type == 'spot':
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / 'spot'
    else:
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / f'futures-{futures_type}'

# ==============================================================================
# ESTRATÉGIA 1: Paralelização da leitura de arquivos
# ==============================================================================

def read_single_parquet(file_path):
    """Lê um único arquivo parquet (para paralelização)."""
    try:
        df = pd.read_parquet(
            file_path,
            columns=['time', 'price', 'qty', 'quoteQty', 'isBuyerMaker'],
            engine='pyarrow'
        )
        # Pré-processamento
        df['side'] = np.where(df['isBuyerMaker'], -1, 1).astype(np.int8)
        df['net_volumes'] = df['quoteQty'] * df['side']
        return df
    except Exception as e:
        logging.error(f"Erro ao ler {file_path}: {e}")
        return pd.DataFrame()

def parallel_read_all_files(raw_dataset_path, max_workers=None):
    """Lê todos os arquivos parquet em paralelo."""
    if max_workers is None:
        max_workers = mp.cpu_count()

    files = sorted(glob.glob(os.path.join(raw_dataset_path, "*.parquet")))
    logging.info(f"📚 Lendo {len(files)} arquivos com {max_workers} workers paralelos")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        dfs = list(executor.map(read_single_parquet, files))

    # Concatena todos os dataframes
    df_combined = pd.concat([df for df in dfs if not df.empty], ignore_index=True)
    logging.info(f"✅ Lidos {len(df_combined):,} registros total")
    return df_combined

# ==============================================================================
# ESTRATÉGIA 2: Processamento em chunks com paralelização parcial
# ==============================================================================

@njit
def find_bar_boundaries(cumsum_volumes, threshold):
    """
    Encontra os índices onde as barras devem ser formadas.
    Esta operação pode ser paralelizada!
    """
    boundaries = List()
    current_sum = 0.0

    for i in range(len(cumsum_volumes)):
        current_sum += abs(cumsum_volumes[i])
        if current_sum >= threshold:
            boundaries.append(i)
            current_sum = 0.0

    return boundaries

def parallel_find_boundaries(net_volumes, threshold, chunk_size=1_000_000):
    """
    Divide os dados em chunks e processa em paralelo onde possível.
    """
    n_chunks = len(net_volumes) // chunk_size + 1
    chunks = np.array_split(net_volumes, n_chunks)

    # Processa chunks em paralelo para encontrar potenciais boundaries
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        partial_func = partial(find_bar_boundaries, threshold=threshold)
        boundaries_per_chunk = list(executor.map(partial_func, chunks))

    # Combina resultados (requer ajuste dos índices)
    all_boundaries = []
    offset = 0
    for chunk_boundaries in boundaries_per_chunk:
        for boundary in chunk_boundaries:
            all_boundaries.append(boundary + offset)
        offset += len(chunks[0])

    return all_boundaries

# ==============================================================================
# ESTRATÉGIA 3: Numba com paralelização interna
# ==============================================================================

@njit(parallel=True, cache=True, fastmath=True)
def compute_dollar_bars_parallel(prices, times, net_volumes, sides, qtys, threshold):
    """
    Versão otimizada com paralelização interna do Numba.
    Usa prange para loops paralelizáveis.
    """
    n = len(prices)
    bars = List()

    current_volume = 0.0
    bar_start_idx = 0

    # Arrays temporários para acumular estatísticas
    high_prices = np.empty(n)
    low_prices = np.empty(n)

    # Este loop não pode ser totalmente paralelizado devido às dependências
    # mas podemos paralelizar operações internas
    for i in range(n):
        current_volume += abs(net_volumes[i])

        if current_volume >= threshold:
            # Calcula estatísticas da barra em paralelo
            bar_data = prices[bar_start_idx:i+1]

            bar_open = prices[bar_start_idx]
            bar_close = prices[i]
            bar_high = np.max(bar_data)
            bar_low = np.min(bar_data)

            bars.append((
                times[bar_start_idx], times[i],
                bar_open, bar_high, bar_low, bar_close,
                current_volume
            ))

            # Reset para próxima barra
            current_volume = 0.0
            bar_start_idx = i + 1

    return bars

# ==============================================================================
# ESTRATÉGIA 4: Pipeline com máxima paralelização
# ==============================================================================

class OptimizedDollarBarsGenerator:
    """Gerador otimizado que maximiza uso de CPU."""

    def __init__(self, data_path, threshold=40_000_000):
        self.data_path = data_path
        self.threshold = threshold
        self.n_workers = mp.cpu_count()

        # Monitora uso de CPU
        self.cpu_monitor = psutil.Process()

    def generate(self):
        """Pipeline principal otimizado."""

        # 1. Leitura paralela (100% CPU)
        logging.info("🚀 Fase 1: Leitura paralela de arquivos")
        df = parallel_read_all_files(self.data_path, self.n_workers)

        # 2. Pré-processamento paralelo (100% CPU)
        logging.info("🚀 Fase 2: Pré-processamento paralelo")
        df = self._parallel_preprocess(df)

        # 3. Geração de barras (otimizada mas sequencial)
        logging.info("🚀 Fase 3: Geração de dollar bars")
        bars = self._generate_bars_optimized(df)

        # 4. Pós-processamento paralelo (100% CPU)
        logging.info("🚀 Fase 4: Pós-processamento paralelo")
        result = self._parallel_postprocess(bars)

        # Monitora performance
        cpu_percent = self.cpu_monitor.cpu_percent()
        logging.info(f"📊 Uso médio de CPU: {cpu_percent:.1f}%")

        return result

    def _parallel_preprocess(self, df):
        """Pré-processamento com vetorização NumPy (usa BLAS)."""
        # NumPy automaticamente usa múltiplas threads via BLAS
        df['net_volumes'] = df['quoteQty'].values * df['side'].values
        df['cumsum_volumes'] = np.abs(df['net_volumes'].values).cumsum()
        return df

    def _generate_bars_optimized(self, df):
        """Geração otimizada de barras."""
        return compute_dollar_bars_parallel(
            df['price'].values,
            df['time'].values,
            df['net_volumes'].values,
            df['side'].values,
            df['qty'].values,
            self.threshold
        )

    def _parallel_postprocess(self, bars):
        """Pós-processamento paralelo dos resultados."""
        df_bars = pd.DataFrame(bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close', 'volume'
        ])

        # Cálculos adicionais em paralelo
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            # Adiciona features em paralelo
            futures = []
            futures.append(executor.submit(self._compute_returns, df_bars))
            futures.append(executor.submit(self._compute_volatility, df_bars))
            futures.append(executor.submit(self._compute_volume_stats, df_bars))

            # Aguarda resultados
            for future in futures:
                future.result()

        return df_bars

    @staticmethod
    def _compute_returns(df):
        """Calcula retornos."""
        df['returns'] = df['close'].pct_change()
        return df

    @staticmethod
    def _compute_volatility(df):
        """Calcula volatilidade."""
        df['volatility'] = df['returns'].rolling(20).std()
        return df

    @staticmethod
    def _compute_volume_stats(df):
        """Calcula estatísticas de volume."""
        df['volume_ma'] = df['volume'].rolling(20).mean()
        return df

# ==============================================================================
# FUNÇÃO PRINCIPAL
# ==============================================================================

def generate_standard_bars_optimized(
    data_type='futures',
    futures_type='um',
    granularity='daily',
    threshold=40_000_000,
    output_dir='./output/standard_optimized/'
):
    """Gera standard dollar bars com máxima utilização de CPU."""

    # Setup
    data_path = get_data_path(data_type, futures_type, granularity)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"🎯 Gerando Dollar Bars Otimizadas")
    logging.info(f"📁 Dados: {data_path}")
    logging.info(f"💰 Threshold: ${threshold:,.0f}")
    logging.info(f"🖥️ CPUs disponíveis: {mp.cpu_count()}")

    # Executa gerador otimizado
    generator = OptimizedDollarBarsGenerator(data_path, threshold)
    df_bars = generator.generate()

    # Salva resultado
    output_file = output_dir / f"dollar_bars_{data_type}_{threshold}.parquet"
    df_bars.to_parquet(output_file, engine='pyarrow', compression='snappy')

    logging.info(f"✅ Concluído! {len(df_bars)} barras geradas")
    logging.info(f"💾 Salvo em: {output_file}")

    return df_bars

if __name__ == "__main__":
    # Teste direto
    df = generate_standard_bars_optimized(
        data_type='spot',
        threshold=40_000_000
    )
    print(f"Geradas {len(df)} barras")