#!/usr/bin/env python3
"""
Advanced Bars Processor - Optimized Implementation with Lopez de Prado's Atoms & Molecules
Based on "Advances in Financial Machine Learning" by Marcos Lopez de Prado

Optimizations include:
1. Numba JIT compilation for computational bottlenecks
2. Vectorized operations with NumPy
3. Parallel chunk processing with multiprocessing
4. Memory-mapped file handling for large datasets
5. Optimized data structures for atoms (ticks) and molecules (bars)
6. Cache-friendly memory access patterns
7. SIMD operations where possible
8. Reduced memory allocations
9. Dask support for distributed processing (NEW)
"""

import sys
import json
import argparse
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import signal
import threading
from pathlib import Path
import shutil
from typing import Dict, Any, Optional, List, Tuple, Iterator
import gc
from dataclasses import dataclass, asdict, field
from enum import Enum
import psutil
from numba import jit, prange, int64, float64
import multiprocessing as mp
import warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)

# Enable Numba caching for faster subsequent runs
import os
os.environ['NUMBA_CACHE_DIR'] = '/tmp/numba_cache'

# Try to import Dask - if not available, we'll use the standard approach
DASK_AVAILABLE = False
try:
    import dask
    import dask.dataframe as dd
    from dask.distributed import Client, as_completed
    from dask.diagnostics import ProgressBar
    DASK_AVAILABLE = True
except ImportError:
    pass


class BarType(Enum):
    """Enumeration of supported bar types"""
    # Standard bars
    TICK = 'tick'
    VOLUME = 'volume'
    DOLLAR = 'dollar'
    # Information-driven bars
    TICK_IMBALANCE = 'tick_imbalance'
    VOLUME_IMBALANCE = 'volume_imbalance'
    DOLLAR_IMBALANCE = 'dollar_imbalance'
    TICK_RUNS = 'tick_runs'
    VOLUME_RUNS = 'volume_runs'
    DOLLAR_RUNS = 'dollar_runs'


@dataclass
class Atom:
    """Represents an atomic unit of market information (tick)"""
    timestamp: np.int64
    price: np.float64
    volume: np.float64
    dollar_volume: np.float64
    tick_rule: np.int64
    
    def to_array(self) -> np.ndarray:
        """Convert to numpy array for efficient processing"""
        return np.array([self.timestamp, self.price, self.volume, 
                        self.dollar_volume, self.tick_rule])


@dataclass
class Molecule:
    """Represents a molecular unit of market information (bar)"""
    open_time: np.int64
    close_time: np.int64
    open: np.float64
    high: np.float64
    low: np.float64
    close: np.float64
    volume: np.float64
    dollar_volume: np.float64
    tick_count: np.int64
    vwap: np.float64
    buy_volume: np.float64
    buy_tick_count: np.int64
    imbalance: np.float64
    bar_type: str
    threshold_value: np.float64
    # Campos dinâmicos dependendo do tipo de bar
    expected_bar_length: Optional[np.float64] = None
    actual_bar_length: Optional[np.int64] = None
    expected_bar_volume: Optional[np.float64] = None
    actual_bar_volume: Optional[np.float64] = None
    expected_bar_dollar_volume: Optional[np.float64] = None
    actual_bar_dollar_volume: Optional[np.float64] = None
    # Campos específicos para imbalance bars
    theta_t: Optional[np.float64] = None
    expected_imbalance: Optional[np.float64] = None
    expected_T: Optional[np.float64] = None


@dataclass
class EWMAState:
    """State for Exponentially Weighted Moving Average calculations"""
    T: Optional[float] = None  # E[T] - Expected bar length
    buy_probability: float = 0.5  # P[b_t = 1]
    expected_b_t: float = 0.0  # E[b_t] = 2*P[b_t=1] - 1
    expected_signed_tick: float = 0.0  # E[b_t] for tick imbalance
    expected_signed_volume: float = 0.0  # E[b_t * v_t] for volume/dollar imbalance
    buy_volume: float = 1.0  # E[v_t|b_t=1] for run bars
    sell_volume: float = 1.0  # E[v_t|b_t=-1] for run bars
    bar_lengths: List[int] = field(default_factory=list)  # Historical bar lengths
    warm_up_count: int = 0  # Number of bars processed
    
    def __post_init__(self):
        self.expected_b_t = 2 * self.buy_probability - 1


@dataclass
class ChunkBuffer:
    """Optimized buffer for maintaining state between chunks"""
    current_bar: Optional[Dict[str, Any]] = None
    accumulator: float = 0.0
    theta_t: float = 0.0  # For information-driven bars
    buy_accumulator: float = 0.0  # For run bars
    sell_accumulator: float = 0.0  # For run bars
    bar_start_idx: int = 0
    processed_rows: int = 0
    last_price: Optional[float] = None
    last_tick_rule: int = 1
    # For tracking statistics within current bar (optimized for memory)
    current_bar_stats: Optional[np.ndarray] = None  # Using numpy array for efficiency
    
    def __post_init__(self):
        if self.current_bar_stats is None:
            self.current_bar_stats = self._init_bar_stats()
    
    def _init_bar_stats(self) -> np.ndarray:
        """Initialize statistics for current bar using numpy array"""
        # Order: buy_count, total_count, buy_volume, sell_volume, 
        #        buy_dollar, sell_dollar, signed_volume_sum, signed_dollar_sum, b_t_sum
        return np.zeros(9, dtype=np.float64)


class ThresholdCache:
    """
    Cache LRU otimizado para cálculo de thresholds
    """
    
    def __init__(self, cache_size: int = 1000):
        self.cache = {}
        self.cache_size = cache_size
        self.access_count = {}
        self.total_accesses = 0
    
    def get_threshold(self, expected_T: float, expected_imbalance: float, 
                     bar_count: int, warm_up_bars: int, bar_type: BarType) -> float:
        """
        Obtém threshold com cache
        """
        # Cria chave com precisão reduzida para melhor hit rate
        key = (
            round(expected_T, 2),
            round(expected_imbalance, 4),
            bar_count < warm_up_bars,
            bar_type.value
        )
        
        if key in self.cache:
            self.access_count[key] = self.total_accesses
            self.total_accesses += 1
            return self.cache[key]
        
        # Calcula threshold
        if bar_count < warm_up_bars:
            # Durante warm-up
            if bar_type == BarType.TICK_IMBALANCE:
                threshold = expected_T * 0.5
            else:
                # Para volume/dollar imbalance, usa estimativa conservadora
                threshold = expected_T * 0.1
        else:
            # Após warm-up
            threshold = expected_T * abs(expected_imbalance)
        
        # Adiciona ao cache com LRU
        self._add_to_cache(key, threshold)
        
        return threshold
    
    def _add_to_cache(self, key, value):
        """Adiciona ao cache com política LRU"""
        if len(self.cache) >= self.cache_size:
            # Remove item menos recentemente usado
            lru_key = min(self.access_count.keys(), 
                         key=lambda k: self.access_count.get(k, -1))
            del self.cache[lru_key]
            del self.access_count[lru_key]
        
        self.cache[key] = value
        self.access_count[key] = self.total_accesses
        self.total_accesses += 1


@jit(nopython=True, cache=True, fastmath=True)
def create_bar_from_data_vectorized(timestamps: np.ndarray, prices: np.ndarray,
                                   volumes: np.ndarray, dollar_volumes: np.ndarray,
                                   tick_rules: np.ndarray, start_idx: int64,
                                   end_idx: int64, threshold: float64,
                                   bar_type_value: int64) -> np.ndarray:
    """
    Cria um bar a partir de dados vetorizados usando operações NumPy otimizadas
    
    Returns:
        Array com dados do bar no formato esperado (15 elementos base)
    """
    # Slice dos dados
    slice_prices = prices[start_idx:end_idx]
    slice_volumes = volumes[start_idx:end_idx]
    slice_dollar_volumes = dollar_volumes[start_idx:end_idx]
    slice_tick_rules = tick_rules[start_idx:end_idx]
    
    # Cálculos vetorizados
    bar_length = end_idx - start_idx
    
    # Check if we have any data
    if bar_length == 0 or len(slice_prices) == 0:
        # Return a bar with zeros/defaults for empty data
        bar_data = np.zeros(15, dtype=np.float64)
        bar_data[0] = float64(timestamps[start_idx] if start_idx < len(timestamps) else 0)
        bar_data[1] = float64(timestamps[start_idx] if start_idx < len(timestamps) else 0)
        bar_data[13] = float64(bar_type_value)
        bar_data[14] = threshold
        return bar_data
    
    total_volume = np.sum(slice_volumes)
    total_dollar_volume = np.sum(slice_dollar_volumes)
    
    # Máscara para buys
    buy_mask = slice_tick_rules > 0
    buy_volume = np.sum(slice_volumes[buy_mask]) if np.any(buy_mask) else 0.0
    buy_count = np.sum(buy_mask)
    
    # VWAP calculation
    vwap = np.sum(slice_prices * slice_volumes) / total_volume if total_volume > 0 else slice_prices[-1]
    
    # Imbalance
    sell_volume = total_volume - buy_volume
    imbalance = (buy_volume - sell_volume) / total_volume if total_volume > 0 else 0.0
    
    # Criar array de saída (15 elementos base)
    bar_data = np.zeros(15, dtype=np.float64)
    
    # Preencher dados
    bar_data[0] = float64(timestamps[start_idx])  # open_time
    bar_data[1] = float64(timestamps[end_idx-1])  # close_time
    bar_data[2] = slice_prices[0]  # open
    bar_data[3] = np.max(slice_prices)  # high
    bar_data[4] = np.min(slice_prices)  # low
    bar_data[5] = slice_prices[-1]  # close
    bar_data[6] = total_volume  # volume
    bar_data[7] = total_dollar_volume  # dollar_volume
    bar_data[8] = float64(bar_length)  # tick_count
    bar_data[9] = vwap  # vwap
    bar_data[10] = buy_volume  # buy_volume
    bar_data[11] = float64(buy_count)  # buy_tick_count
    bar_data[12] = imbalance  # imbalance
    bar_data[13] = float64(bar_type_value)  # bar_type (será convertido para string depois)
    bar_data[14] = threshold  # threshold_value
    
    return bar_data


class EWMALazyUpdater:
    """
    Implementa atualização preguiçosa (lazy) de EWMA para melhor performance
    Acumula observações e atualiza em batch
    """
    
    def __init__(self, ewma_state: EWMAState, alpha_T: float, alpha_imbalance: float, 
                 update_frequency: int = 20):
        self.ewma_state = ewma_state
        self.alpha_T = alpha_T
        self.alpha_imbalance = alpha_imbalance
        self.update_frequency = update_frequency
        self.pending_updates = []
        self.update_counter = 0
        
        # Para estatísticas acumuladas
        self.accumulated_lengths = []
        self.accumulated_imbalances = []
        self.accumulated_buy_probs = []
    
    def add_observation(self, bar_length: int, bar_stats: np.ndarray, theta_t: float):
        """Adiciona observação para atualização posterior"""
        self.accumulated_lengths.append(bar_length)
        
        # Calcula estatísticas do bar
        if bar_stats[1] > 0:  # total_count > 0
            buy_prob = bar_stats[0] / bar_stats[1]
            self.accumulated_buy_probs.append(buy_prob)
            
            # Para imbalance bars
            avg_imbalance = theta_t / bar_length if bar_length > 0 else 0
            self.accumulated_imbalances.append(avg_imbalance)
        
        self.update_counter += 1
        
        # Atualiza quando atinge a frequência
        if self.update_counter >= self.update_frequency:
            self._batch_update()
    
    def _batch_update(self):
        """Executa atualização em batch do EWMA"""
        if not self.accumulated_lengths:
            return
        
        # Converte para arrays NumPy para vetorização
        lengths = np.array(self.accumulated_lengths, dtype=np.float64)
        
        # Calcula médias ponderadas
        avg_length = np.mean(lengths)
        
        # Atualiza E[T] com alpha_T
        self.ewma_state.T = update_ewma(self.alpha_T, self.ewma_state.T, avg_length)
        
        # Atualiza estatísticas de imbalance
        if self.accumulated_imbalances:
            imbalances = np.array(self.accumulated_imbalances, dtype=np.float64)
            avg_imbalance = np.mean(imbalances)
            
            # Atualiza expected_signed_volume/tick com alpha_imbalance
            self.ewma_state.expected_signed_volume = update_ewma(
                self.alpha_imbalance, 
                self.ewma_state.expected_signed_volume, 
                avg_imbalance
            )
        
        # Atualiza probabilidade de compra
        if self.accumulated_buy_probs:
            buy_probs = np.array(self.accumulated_buy_probs, dtype=np.float64)
            avg_buy_prob = np.mean(buy_probs)
            
            self.ewma_state.buy_probability = update_ewma(
                self.alpha_imbalance,
                self.ewma_state.buy_probability,
                avg_buy_prob
            )
            self.ewma_state.expected_b_t = 2 * self.ewma_state.buy_probability - 1
        
        # Incrementa contador de warm-up
        self.ewma_state.warm_up_count += self.update_counter
        
        # Limpa acumuladores
        self.accumulated_lengths = []
        self.accumulated_imbalances = []
        self.accumulated_buy_probs = []
        self.update_counter = 0
    
    def force_update(self):
        """Força atualização imediata das estatísticas pendentes"""
        if self.update_counter > 0:
            self._batch_update()


# Numba-optimized functions for performance-critical operations
@jit(nopython=True, cache=True, fastmath=True)
def apply_tick_rule_vectorized(prices: np.ndarray, 
                               prev_price: float64,
                               prev_tick_rule: int64) -> np.ndarray:
    """
    Vectorized tick rule application using Numba
    b_t = { b_{t-1} if Δp_t = 0
          { sign(Δp_t) if Δp_t ≠ 0
    """
    n = len(prices)
    tick_rules = np.zeros(n, dtype=np.int64)
    
    # Handle first tick
    if n > 0:
        price_diff = prices[0] - prev_price
        if price_diff != 0:
            # Explicitly cast to int64
            tick_rules[0] = np.int64(1) if price_diff > 0 else np.int64(-1)
        else:
            tick_rules[0] = prev_tick_rule
    
    # Vectorized processing for remaining ticks
    for i in prange(1, n):
        price_diff = prices[i] - prices[i-1]
        if price_diff != 0:
            # Explicitly cast to int64
            tick_rules[i] = np.int64(1) if price_diff > 0 else np.int64(-1)
        else:
            tick_rules[i] = tick_rules[i-1]
    
    return tick_rules


@jit(nopython=True, cache=True, fastmath=True)
def process_imbalance_batch(prices: np.ndarray, volumes: np.ndarray, 
                           dollar_volumes: np.ndarray, tick_rules: np.ndarray,
                           current_theta: float64, threshold: float64,
                           bar_type_id: int64, batch_size: int64 = 10) -> tuple[int64, float64]:
    """
    Processa um batch de ticks para imbalance bars com verificação esparsa
    
    Args:
        prices: Array de preços
        volumes: Array de volumes
        dollar_volumes: Array de dollar volumes
        tick_rules: Array de tick rules (-1, 1)
        current_theta: Valor acumulado de theta
        threshold: Threshold atual para formar bar
        bar_type_id: 0=TICK_IMBALANCE, 1=VOLUME_IMBALANCE, 2=DOLLAR_IMBALANCE
        batch_size: Tamanho do micro-batch para verificação
    
    Returns:
        (índice onde condição foi satisfeita ou -1, theta acumulado)
    """
    n = len(prices)
    theta = current_theta
    
    # Processa em micro-batches para eficiência
    for i in range(0, n, batch_size):
        end_idx = min(i + batch_size, n)
        
        # Acumula theta para o batch
        if bar_type_id == 0:  # TICK_IMBALANCE
            batch_theta = np.sum(tick_rules[i:end_idx])
        elif bar_type_id == 1:  # VOLUME_IMBALANCE
            batch_theta = np.sum(tick_rules[i:end_idx] * volumes[i:end_idx])
        else:  # DOLLAR_IMBALANCE
            batch_theta = np.sum(tick_rules[i:end_idx] * dollar_volumes[i:end_idx])
        
        theta += batch_theta
        
        # Verifica condição apenas no final do batch
        if abs(theta) >= threshold:
            # Refina para encontrar o ponto exato
            theta = current_theta
            for j in range(i, end_idx):
                if bar_type_id == 0:
                    theta += tick_rules[j]
                elif bar_type_id == 1:
                    theta += tick_rules[j] * volumes[j]
                else:
                    theta += tick_rules[j] * dollar_volumes[j]
                
                if abs(theta) >= threshold:
                    return j, theta
    
    return int64(-1), theta


@jit(nopython=True, cache=True, fastmath=True)
def update_bar_numba(bar_open: float64, bar_high: float64, bar_low: float64,
                     bar_volume: float64, bar_dollar_volume: float64,
                     bar_tick_count: int64, bar_buy_volume: float64,
                     bar_buy_tick_count: int64, vwap_num: float64,
                     price: float64, volume: float64, dollar_volume: float64,
                     tick_rule: int64) -> Tuple[float64, float64, float64, float64, 
                                               float64, int64, float64, int64, float64]:
    """Numba-optimized bar update"""
    bar_high = max(bar_high, price)
    bar_low = min(bar_low, price)
    bar_volume += volume
    bar_dollar_volume += dollar_volume
    bar_tick_count += 1
    
    if tick_rule > 0:
        bar_buy_volume += volume
        bar_buy_tick_count += 1
    
    vwap_num += price * volume
    
    return (bar_high, bar_low, bar_volume, bar_dollar_volume,
            bar_tick_count, bar_buy_volume, bar_buy_tick_count, vwap_num)


@jit(nopython=True, cache=True, fastmath=True)
def calculate_ewma_alpha(span: int64) -> float64:
    """Calculate EWMA alpha factor"""
    return 2.0 / (span + 1)


@jit(nopython=True, cache=True, fastmath=True)
def update_ewma(alpha: float64, old_value: float64, new_value: float64) -> float64:
    """Update value using Exponentially Weighted Moving Average"""
    return alpha * new_value + (1 - alpha) * old_value


@jit(nopython=True, cache=True, fastmath=True)
def update_bar_stats_numba(stats: np.ndarray, volume: float64, 
                          dollar_volume: float64, tick_rule: int64):
    """Numba-optimized bar statistics update"""
    stats[1] += 1  # total_count
    
    if tick_rule > 0:
        stats[0] += 1  # buy_count
        stats[2] += volume  # buy_volume
        stats[4] += dollar_volume  # buy_dollar
    else:
        stats[3] += volume  # sell_volume
        stats[5] += dollar_volume  # sell_dollar
    
    stats[6] += tick_rule * volume  # signed_volume_sum
    stats[7] += tick_rule * dollar_volume  # signed_dollar_sum
    stats[8] += tick_rule  # b_t_sum


# Numba-optimized vectorized processing functions
@jit(nopython=True, cache=True, fastmath=True, parallel=True)
def process_tick_bars_vectorized(timestamps: np.ndarray, prices: np.ndarray, 
                                volumes: np.ndarray, dollar_volumes: np.ndarray,
                                tick_rules: np.ndarray, threshold: int64,
                                start_idx: int64) -> Tuple[np.ndarray, int64]:
    """Vectorized processing for tick bars"""
    n = len(prices)
    bar_boundaries = np.zeros(n // threshold + 2, dtype=np.int64)
    bar_count = 0
    current_ticks = 0
    
    for i in prange(n):
        current_ticks += 1
        if current_ticks >= threshold:
            bar_boundaries[bar_count] = start_idx + i + 1
            bar_count += 1
            current_ticks = 0
    
    return bar_boundaries[:bar_count], start_idx + n


@jit(nopython=True, cache=True, fastmath=True, parallel=True)
def process_volume_bars_vectorized(volumes: np.ndarray, threshold: float64,
                                  start_idx: int64) -> Tuple[np.ndarray, float64, int64]:
    """Vectorized processing for volume bars"""
    n = len(volumes)
    cumsum = np.cumsum(volumes)
    num_bars = int(cumsum[-1] / threshold) + 1
    bar_boundaries = np.zeros(num_bars, dtype=np.int64)
    
    current_threshold = threshold
    bar_count = 0
    
    for i in prange(n):
        if cumsum[i] >= current_threshold:
            bar_boundaries[bar_count] = start_idx + i + 1
            bar_count += 1
            current_threshold += threshold
    
    remaining = cumsum[-1] - (bar_count * threshold)
    return bar_boundaries[:bar_count], remaining, start_idx + n


class AdvancedBarsProcessor:
    """
    Main processor class implementing Lopez de Prado's bar sampling methodology
    with optimizations for atoms and molecules processing
    """
    
    def __init__(self, task_id: str, dataset_path: str, bar_type: str, 
                 parameters: Dict[str, Any], output_dir: str):
        self.task_id = task_id
        # Handle relative paths - if path doesn't exist as absolute, try relative to backend
        self.dataset_path = Path(dataset_path)
        if not self.dataset_path.is_absolute() or not self.dataset_path.exists():
            # Try relative to backend directory
            backend_path = Path('/ors/workdir/OrionQuant/backend') / dataset_path
            if backend_path.exists():
                self.dataset_path = backend_path
            else:
                self.dataset_path = Path(dataset_path)
        
        self.bar_type = BarType(bar_type)
        self.parameters = parameters
        self.output_dir = Path(output_dir)
        self.temp_dir = self.output_dir / "temp"
        
        # Validate input file
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {dataset_path} (resolved to: {self.dataset_path})")
        
        # Create necessary directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Execution control
        self.cancelled = False
        self.paused = False
        self.pause_event = threading.Event()
        self.pause_event.set()  # Start unpaused
        
        # State persistence
        self.state_file = self.output_dir / "bars_state.json"
        self.processed_chunks = set()
        
        # Initialize states
        self.ewma_state = EWMAState()
        self.chunk_buffer = ChunkBuffer()
        
        # Memory optimization parameters
        self._configure_memory_settings()
        
        # Parallel processing settings
        self.n_workers = min(mp.cpu_count() - 1, self.parameters.get('n_workers', 4))
        self.parallel_enabled = self.parameters.get('parallel_processing', True)
        
        # Dask settings
        self.use_dask = self.parameters.get('use_dask', False) and DASK_AVAILABLE
        self.dask_client = None
        if self.use_dask:
            self._setup_dask_client()
        
        # Batch processing for incremental saves
        self.batch_number = 0
        self.total_bars_generated = 0
        self.bars_metadata = []  # Store only metadata, not full bars
        self.pending_bars = []  # Store bars that haven't been saved yet
        
        # Pre-allocate arrays for efficiency
        self.bar_array_buffer = np.zeros((self.bars_per_batch, 18), dtype=np.float64)
        self.bar_buffer_pos = 0
        
        # Load saved state if exists
        self._load_state()
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._handle_cancel_signal)
        signal.signal(signal.SIGINT, self._handle_cancel_signal)
        signal.signal(signal.SIGUSR1, self._handle_pause_signal)
        signal.signal(signal.SIGUSR2, self._handle_resume_signal)
        
        # Output schema for bars - agora dinâmico
        self.output_schema = self._get_dynamic_schema()
        
        # Validate parameters
        self._validate_parameters()
        
        # Initialize EWMA state with parameters
        self._initialize_ewma_state()
    
    def _get_dynamic_schema(self) -> pa.Schema:
        """Cria schema específico baseado no tipo de bar"""
        base_fields = [
            ('open_time', pa.timestamp('ns')),
            ('close_time', pa.timestamp('ns')),
            ('open', pa.float64()),
            ('high', pa.float64()),
            ('low', pa.float64()),
            ('close', pa.float64()),
            ('volume', pa.float64()),
            ('dollar_volume', pa.float64()),
            ('tick_count', pa.int64()),
            ('vwap', pa.float64()),
            ('buy_volume', pa.float64()),
            ('buy_tick_count', pa.int64()),
            ('imbalance', pa.float64()),
            ('bar_type', pa.string()),
            ('threshold_value', pa.float64())
        ]
        
        # Adicionar campos específicos baseado no tipo de bar
        if self.bar_type == BarType.TICK or self.bar_type == BarType.TICK_IMBALANCE or self.bar_type == BarType.TICK_RUNS:
            base_fields.extend([
                ('expected_bar_length', pa.float64()),
                ('actual_bar_length', pa.int64())
            ])
        elif self.bar_type == BarType.VOLUME or self.bar_type == BarType.VOLUME_IMBALANCE or self.bar_type == BarType.VOLUME_RUNS:
            base_fields.extend([
                ('expected_bar_volume', pa.float64()),
                ('actual_bar_volume', pa.float64())
            ])
        elif self.bar_type == BarType.DOLLAR or self.bar_type == BarType.DOLLAR_IMBALANCE or self.bar_type == BarType.DOLLAR_RUNS:
            base_fields.extend([
                ('expected_bar_dollar_volume', pa.float64()),
                ('actual_bar_dollar_volume', pa.float64())
            ])
        
        # Adicionar campos específicos para imbalance bars
        if 'imbalance' in self.bar_type.value:
            base_fields.extend([
                ('theta_t', pa.float64()),  # Valor acumulado atual
                ('expected_imbalance', pa.float64()),  # E[θ_T]/E[T]
                ('expected_T', pa.float64())  # E[T]
            ])
        
        return pa.schema(base_fields)

    def _setup_dask_client(self):
        """Setup Dask client for distributed processing"""
        try:
            # Check if client already exists
            try:
                self.dask_client = Client.current()
                self.log_progress(f"Connected to existing Dask cluster with {len(self.dask_client.scheduler_info()['workers'])} workers", status="info")
            except:
                # Create local client with optimal settings
                n_workers = self.parameters.get('dask_workers', min(4, mp.cpu_count() - 1))
                threads_per_worker = self.parameters.get('dask_threads_per_worker', 2)
                memory_limit = self.parameters.get('dask_memory_limit', 'auto')
                
                self.dask_client = Client(
                    n_workers=n_workers,
                    threads_per_worker=threads_per_worker,
                    memory_limit=memory_limit,
                    processes=True,
                    silence_logs=40  # Only show warnings and errors
                )
                self.log_progress(f"Created Dask LocalCluster with {n_workers} workers", status="info")
        except Exception as e:
            self.log_progress(f"Failed to setup Dask client: {e}. Falling back to standard processing.", status="warning")
            self.use_dask = False
            self.dask_client = None

    def _configure_memory_settings(self):
        """Configure memory settings based on available resources"""
        try:
            available_memory_gb = psutil.virtual_memory().available / (1024**3)
            
            # Adjust parameters based on available memory
            if available_memory_gb < 4:
                self.chunk_size = 100_000
                self.bars_per_batch = 500
                self.log_progress(f"Low memory mode: {available_memory_gb:.1f}GB available")
            elif available_memory_gb < 8:
                self.chunk_size = 500_000
                self.bars_per_batch = 1000
                self.log_progress(f"Medium memory mode: {available_memory_gb:.1f}GB available")
            else:
                self.chunk_size = 1_000_000
                self.bars_per_batch = 5000
                self.log_progress(f"High memory mode: {available_memory_gb:.1f}GB available")
            
            # Allow override from parameters
            self.chunk_size = self.parameters.get('chunk_size', self.chunk_size)
            self.bars_per_batch = self.parameters.get('bars_per_batch', self.bars_per_batch)
            self.use_chunking = self.parameters.get('use_chunking', True)
            
            # Dask-specific settings
            if self.use_dask:
                # Larger chunks for Dask to reduce overhead
                self.dask_chunk_size = self.parameters.get('dask_chunk_size', self.chunk_size * 10)
                self.dask_partition_size = self.parameters.get('dask_partition_size', '128MB')
            
        except Exception as e:
            # Fallback to default values if psutil fails
            self.chunk_size = self.parameters.get('chunk_size', 500_000)
            self.bars_per_batch = self.parameters.get('bars_per_batch', 1000)
            self.use_chunking = self.parameters.get('use_chunking', True)
            self.log_progress(f"Using default memory settings: {e}")

    def _check_memory_usage(self):
        """Check memory usage and force garbage collection if necessary"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / (1024 * 1024)
            
            if memory_mb > 2000:  # If using more than 2GB
                self.log_progress(f"High memory usage detected: {memory_mb:.1f}MB. Running cleanup...", status="info")
                gc.collect()
                
                # Check again
                memory_mb_after = process.memory_info().rss / (1024 * 1024)
                self.log_progress(f"Memory after cleanup: {memory_mb_after:.1f}MB", status="info")
                
                # If still high after cleanup, raise warning
                if memory_mb_after > 3000:  # 3GB
                    self.log_progress(f"WARNING: Memory usage still high after cleanup: {memory_mb_after:.1f}MB", status="warning")
        except:
            # If psutil fails, just run gc anyway
            gc.collect()

    def _validate_parameters(self):
        """Validate all input parameters according to Lopez de Prado's specifications"""
        # For standard bars, validate thresholds
        if self.bar_type in [BarType.TICK, BarType.VOLUME, BarType.DOLLAR]:
            threshold_map = {
                BarType.TICK: 'tick_threshold',
                BarType.VOLUME: 'volume_threshold',
                BarType.DOLLAR: 'dollar_threshold'
            }
            threshold_key = threshold_map[self.bar_type]
            
            if threshold_key not in self.parameters and 'threshold' not in self.parameters:
                raise ValueError(f"Required parameter '{threshold_key}' or 'threshold' not provided")
            
            threshold = self.parameters.get(threshold_key, self.parameters.get('threshold'))
            if not isinstance(threshold, (int, float)) or threshold <= 0:
                raise ValueError(f"Threshold must be positive, got: {threshold}")
            
            # Ensure specific threshold key is set
            self.parameters[threshold_key] = threshold
        
        # Validate EWMA spans for different bar types
        if self._is_information_driven():
            # Check for dual EWMA spans for certain bar types
            if self.bar_type in [BarType.TICK_IMBALANCE, BarType.VOLUME_IMBALANCE, 
                               BarType.DOLLAR_IMBALANCE, BarType.TICK_RUNS]:
                # These bar types need two separate EWMA spans
                ewma_span_T = self.parameters.get('ewma_span_T', self.parameters.get('ewma_span', 100))
                if not isinstance(ewma_span_T, int) or ewma_span_T < 1:
                    raise ValueError(f"ewma_span_T must be integer >= 1, got: {ewma_span_T}")
                self.parameters['ewma_span_T'] = ewma_span_T
                
                # Second span for imbalance/probability
                if self.bar_type in [BarType.TICK_IMBALANCE, BarType.VOLUME_IMBALANCE, BarType.DOLLAR_IMBALANCE]:
                    ewma_span_imbalance = self.parameters.get('ewma_span_imbalance', self.parameters.get('ewma_span', 100))
                    if not isinstance(ewma_span_imbalance, int) or ewma_span_imbalance < 1:
                        raise ValueError(f"ewma_span_imbalance must be integer >= 1, got: {ewma_span_imbalance}")
                    self.parameters['ewma_span_imbalance'] = ewma_span_imbalance
                elif self.bar_type == BarType.TICK_RUNS:
                    ewma_span_probability = self.parameters.get('ewma_span_probability', self.parameters.get('ewma_span', 100))
                    if not isinstance(ewma_span_probability, int) or ewma_span_probability < 1:
                        raise ValueError(f"ewma_span_probability must be integer >= 1, got: {ewma_span_probability}")
                    self.parameters['ewma_span_probability'] = ewma_span_probability
            
            elif self.bar_type in [BarType.VOLUME_RUNS, BarType.DOLLAR_RUNS]:
                # These use span for T and span for volumes
                ewma_span_T = self.parameters.get('ewma_span_T', self.parameters.get('ewma_span', 100))
                if not isinstance(ewma_span_T, int) or ewma_span_T < 1:
                    raise ValueError(f"ewma_span_T must be integer >= 1, got: {ewma_span_T}")
                self.parameters['ewma_span_T'] = ewma_span_T
                
                ewma_span_volumes = self.parameters.get('ewma_span_volumes', self.parameters.get('ewma_span', 100))
                if not isinstance(ewma_span_volumes, int) or ewma_span_volumes < 1:
                    raise ValueError(f"ewma_span_volumes must be integer >= 1, got: {ewma_span_volumes}")
                self.parameters['ewma_span_volumes'] = ewma_span_volumes
        else:
            # Standard bars may still use single span for compatibility
            ewma_span = self.parameters.get('ewma_span', 100)
            if not isinstance(ewma_span, int) or ewma_span < 1:
                raise ValueError(f"ewma_span must be integer >= 1, got: {ewma_span}")
        
        warm_up_bars = self.parameters.get('warm_up_bars', 20)
        if not isinstance(warm_up_bars, int) or warm_up_bars < 0:
            raise ValueError(f"warm_up_bars must be integer >= 0, got: {warm_up_bars}")
        
        # Validate initial buy probability
        init_buy_prob = self.parameters.get('initial_buy_probability', 0.5)
        if not 0 < init_buy_prob < 1:
            raise ValueError(f"initial_buy_probability must be in (0, 1), got: {init_buy_prob}")
        
        # For information-driven bars, validate initial expectations
        if self._is_information_driven():
            self._validate_information_driven_parameters()
        
        self.log_progress("All parameters validated successfully", status="info")

    def _validate_information_driven_parameters(self):
        """Validate parameters specific to information-driven bars"""
        # Map bar types to their initial expectation parameters
        expectation_map = {
            BarType.TICK_IMBALANCE: 'initial_expected_ticks',
            BarType.TICK_RUNS: 'initial_expected_ticks',
            BarType.VOLUME_IMBALANCE: 'initial_expected_volume',
            BarType.VOLUME_RUNS: 'initial_expected_volume',
            BarType.DOLLAR_IMBALANCE: 'initial_expected_dollar',
            BarType.DOLLAR_RUNS: 'initial_expected_dollar'
        }
        
        expected_key = expectation_map.get(self.bar_type)
        if expected_key and expected_key in self.parameters:
            value = self.parameters[expected_key]
            if not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{expected_key} must be positive, got: {value}")
        
        # For imbalance bars (not tick), validate initial expected signed volume
        if 'imbalance' in self.bar_type.value and self.bar_type != BarType.TICK_IMBALANCE:
            signed_vol = self.parameters.get('initial_expected_signed_volume', 0.0)
            if not isinstance(signed_vol, (int, float)):
                raise ValueError(f"initial_expected_signed_volume must be numeric, got: {signed_vol}")

    def _initialize_ewma_state(self):
        """Initialize EWMA state with parameters according to bar type"""
        self.ewma_state.buy_probability = self.parameters.get('initial_buy_probability', 0.5)
        self.ewma_state.expected_b_t = 2 * self.ewma_state.buy_probability - 1
        
        # Set initial expected bar length based on bar type
        if self.bar_type == BarType.TICK_IMBALANCE:
            self.ewma_state.T = self.parameters.get('initial_expected_ticks', 1000)
            self.ewma_state.expected_signed_tick = self.ewma_state.expected_b_t
        elif self.bar_type == BarType.TICK_RUNS:
            self.ewma_state.T = self.parameters.get('initial_expected_ticks', 1000)
        elif self.bar_type == BarType.VOLUME_IMBALANCE:
            self.ewma_state.T = self.parameters.get('initial_expected_volume', 1000)
            self.ewma_state.expected_signed_volume = self.parameters.get('initial_expected_signed_volume', 0.0)
        elif self.bar_type == BarType.VOLUME_RUNS:
            self.ewma_state.T = self.parameters.get('initial_expected_volume', 1000)
            self.ewma_state.buy_volume = self.parameters.get('initial_buy_volume', 1.0)
            self.ewma_state.sell_volume = self.parameters.get('initial_sell_volume', 1.0)
        elif self.bar_type == BarType.DOLLAR_IMBALANCE:
            self.ewma_state.T = self.parameters.get('initial_expected_dollar', 10000)
            self.ewma_state.expected_signed_volume = self.parameters.get('initial_expected_signed_volume', 0.0)
        elif self.bar_type == BarType.DOLLAR_RUNS:
            self.ewma_state.T = self.parameters.get('initial_expected_dollar', 10000)
            self.ewma_state.buy_volume = self.parameters.get('initial_buy_volume', 10.0)
            self.ewma_state.sell_volume = self.parameters.get('initial_sell_volume', 10.0)

    def _is_information_driven(self) -> bool:
        """Check if current bar type is information-driven"""
        return 'imbalance' in self.bar_type.value or 'runs' in self.bar_type.value

    def _handle_cancel_signal(self, signum, frame):
        """Handle cancellation signals"""
        self.cancelled = True
        self.log_progress(f"Received signal {signum}. Cancelling processing...", status="cancelled")
        if self.dask_client:
            self.dask_client.close()
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        sys.exit(0)

    def _handle_pause_signal(self, signum, frame):
        """Handle pause signal (SIGUSR1)"""
        if not self.paused:
            self.paused = True
            self.pause_event.clear()
            self.log_progress("Processing paused", status="paused")
            self._save_state()

    def _handle_resume_signal(self, signum, frame):
        """Handle resume signal (SIGUSR2)"""
        if self.paused:
            self.paused = False
            self.pause_event.set()
            self.log_progress("Processing resumed", status="processing")

    def _save_state(self):
        """Save current processing state for recovery"""
        # Convert numpy array to list for JSON serialization
        bar_stats_list = self.chunk_buffer.current_bar_stats.tolist() if isinstance(self.chunk_buffer.current_bar_stats, np.ndarray) else self.chunk_buffer.current_bar_stats
        
        state = {
            "task_id": self.task_id,
            "processed_chunks": list(self.processed_chunks),
            "bar_type": self.bar_type.value,
            "parameters": self.parameters,
            "ewma_state": asdict(self.ewma_state),
            "chunk_buffer": {
                "accumulator": self.chunk_buffer.accumulator,
                "theta_t": self.chunk_buffer.theta_t,
                "buy_accumulator": self.chunk_buffer.buy_accumulator,
                "sell_accumulator": self.chunk_buffer.sell_accumulator,
                "bar_start_idx": self.chunk_buffer.bar_start_idx,
                "processed_rows": self.chunk_buffer.processed_rows,
                "last_price": self.chunk_buffer.last_price,
                "last_tick_rule": self.chunk_buffer.last_tick_rule,
                "current_bar_stats": bar_stats_list
            },
            "batch_number": self.batch_number,
            "total_bars_generated": self.total_bars_generated,
            "pending_bars_count": len(self.pending_bars),
            "last_update": datetime.now().isoformat()
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2, default=str)

    def _load_state(self):
        """Load saved processing state"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    
                self.processed_chunks = set(state.get("processed_chunks", []))
                self.batch_number = int(state.get("batch_number", 0))
                self.total_bars_generated = int(state.get("total_bars_generated", 0))
                
                # Note: We can't restore pending_bars from state as they would be too large
                # But we should log if there were pending bars
                pending_bars_count = state.get("pending_bars_count", 0)
                if pending_bars_count > 0:
                    self.log_progress(f"Note: {pending_bars_count} bars were pending but not saved in previous run", status="warning")
                
                # Load EWMA state
                if "ewma_state" in state:
                    ewma_data = state["ewma_state"]
                    self.ewma_state = EWMAState(**ewma_data)
                
                # Load chunk buffer
                if "chunk_buffer" in state:
                    buffer_data = state["chunk_buffer"]
                    self.chunk_buffer.accumulator = float(buffer_data.get("accumulator", 0))
                    self.chunk_buffer.theta_t = float(buffer_data.get("theta_t", 0))
                    self.chunk_buffer.buy_accumulator = float(buffer_data.get("buy_accumulator", 0))
                    self.chunk_buffer.sell_accumulator = float(buffer_data.get("sell_accumulator", 0))
                    self.chunk_buffer.bar_start_idx = int(buffer_data.get("bar_start_idx", 0))
                    self.chunk_buffer.processed_rows = int(buffer_data.get("processed_rows", 0))
                    self.chunk_buffer.last_price = buffer_data.get("last_price")
                    self.chunk_buffer.last_tick_rule = int(buffer_data.get("last_tick_rule", 1))
                    
                    # Convert bar stats back to numpy array
                    bar_stats_list = buffer_data.get("current_bar_stats")
                    if bar_stats_list:
                        self.chunk_buffer.current_bar_stats = np.array(bar_stats_list, dtype=np.float64)
                    else:
                        self.chunk_buffer.current_bar_stats = self.chunk_buffer._init_bar_stats()
                
                if self.processed_chunks:
                    self.log_progress(
                        f"Resuming processing - {len(self.processed_chunks)} chunks already processed, "
                        f"{self.total_bars_generated} bars generated"
                    )
            except Exception as e:
                self.log_progress(f"Error loading state: {e}", status="warning")

    def log_progress(self, message: str, percentage: Optional[float] = None, 
                    status: str = "processing"):
        """Log progress in JSON format"""
        progress_data = {
            "task_id": self.task_id,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        if percentage is not None:
            progress_data["percentage"] = round(percentage, 2)
            
        print(json.dumps(progress_data, default=str), flush=True)

    def load_data_chunked(self) -> Iterator[Tuple[int, pd.DataFrame]]:
        """Load data in chunks for memory efficiency with parallel processing support"""
        try:
            file_size = self.dataset_path.stat().st_size
            
            if self.use_chunking and file_size > 100 * 1024 * 1024:  # > 100MB
                self.log_progress("Loading dataset in chunked mode for memory optimization...")
                
                parquet_file = pq.ParquetFile(self.dataset_path)
                num_row_groups = parquet_file.num_row_groups
                
                if num_row_groups > 1:
                    # Process by row groups
                    for i in range(num_row_groups):
                        if i in self.processed_chunks:
                            continue
                        
                        chunk = parquet_file.read_row_group(i).to_pandas()
                        chunk = self._preprocess_chunk(chunk)
                        yield i, chunk
                        
                        self.processed_chunks.add(i)
                        self._save_state()
                        gc.collect()
                else:
                    # Manual chunking for single row group with memory mapping
                    df = pd.read_parquet(self.dataset_path)
                    total_rows = len(df)
                    
                    for i in range(0, total_rows, self.chunk_size):
                        chunk_id = i // self.chunk_size
                        if chunk_id in self.processed_chunks:
                            continue
                        
                        chunk = df.iloc[i:i + self.chunk_size].copy()
                        chunk = self._preprocess_chunk(chunk)
                        yield chunk_id, chunk
                        
                        self.processed_chunks.add(chunk_id)
                        self._save_state()
                        del chunk
                        gc.collect()
            else:
                # Small files - process all at once
                self.log_progress("Loading complete dataset...")
                df = pd.read_parquet(self.dataset_path)
                df = self._preprocess_chunk(df)
                yield 0, df
                
        except Exception as e:
            raise Exception(f"Error loading dataset: {str(e)}")

    def load_data_with_dask(self) -> dd.DataFrame:
        """Load data using Dask for distributed processing"""
        try:
            self.log_progress(f"Loading dataset with Dask (partition size: {self.dask_partition_size})...")
            
            # Read parquet file with Dask
            ddf = dd.read_parquet(
                self.dataset_path,
                engine='pyarrow',
                chunksize=self.dask_partition_size,
                columns=None  # Read all columns
            )
            
            # Map columns to standard names
            column_mapping = {
                'time': 'timestamp',
                'qty': 'volume',
                'quote_qty': 'dollar_volume'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in ddf.columns:
                    ddf = ddf.rename(columns={old_col: new_col})
            
            # Validate required columns
            required_columns = ['timestamp', 'price', 'volume']
            missing_columns = [col for col in required_columns if col not in ddf.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Convert timestamp if necessary
            if ddf['timestamp'].dtype != 'datetime64[ns]':
                ddf['timestamp'] = dd.to_datetime(ddf['timestamp'])
            
            # Calculate dollar volume if not present
            if 'dollar_volume' not in ddf.columns:
                ddf['dollar_volume'] = ddf['price'] * ddf['volume']
            
            # Ensure data is sorted by timestamp
            ddf = ddf.set_index('timestamp', sorted=True)
            
            # Optimize partitions
            n_partitions = ddf.npartitions
            optimal_partitions = max(1, min(n_partitions, self.n_workers * 4))
            if n_partitions != optimal_partitions:
                self.log_progress(f"Repartitioning from {n_partitions} to {optimal_partitions} partitions...")
                ddf = ddf.repartition(npartitions=optimal_partitions)
            
            self.log_progress(f"Dask DataFrame loaded with {ddf.npartitions} partitions")
            return ddf
            
        except Exception as e:
            self.log_progress(f"Error loading with Dask: {e}. Falling back to standard loading.", status="warning")
            self.use_dask = False
            raise

    def _preprocess_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Preprocess a chunk of data with proper tick rule application"""
        # Map columns to standard names
        column_mapping = {
            'time': 'timestamp',
            'qty': 'volume',
            'quote_qty': 'dollar_volume'
        }
        
        chunk = chunk.rename(columns=column_mapping)
        
        # Validate required columns
        required_columns = ['timestamp', 'price', 'volume']
        missing_columns = [col for col in required_columns if col not in chunk.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert timestamp if necessary
        if not pd.api.types.is_datetime64_any_dtype(chunk['timestamp']):
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])
        
        # Sort by timestamp
        chunk = chunk.sort_values('timestamp').reset_index(drop=True)
        
        # Calculate dollar volume if not present
        if 'dollar_volume' not in chunk.columns:
            chunk['dollar_volume'] = chunk['price'] * chunk['volume']
        
        # Apply tick rule using vectorized function
        prices_array = chunk['price'].values.astype(np.float64)
        chunk['tick_rule'] = apply_tick_rule_vectorized(
            prices_array,
            self.chunk_buffer.last_price if self.chunk_buffer.last_price is not None else prices_array[0],
            self.chunk_buffer.last_tick_rule
        )
        
        # Update buffer with last values for next chunk
        if len(chunk) > 0:
            self.chunk_buffer.last_price = chunk.iloc[-1]['price']
            self.chunk_buffer.last_tick_rule = chunk.iloc[-1]['tick_rule']
        
        return chunk

    def _preprocess_dask_partition(self, partition: pd.DataFrame, prev_price: float, prev_tick_rule: int) -> pd.DataFrame:
        """Preprocess a Dask partition with tick rule application"""
        # Apply tick rule
        prices_array = partition['price'].values.astype(np.float64)
        partition['tick_rule'] = apply_tick_rule_vectorized(prices_array, prev_price, prev_tick_rule)
        
        return partition

    def _apply_tick_rule(self, df: pd.DataFrame, 
                        previous_price: Optional[float] = None,
                        previous_tick_rule: int = 1) -> pd.Series:
        """
        Apply tick rule according to Lopez de Prado
        b_t = { b_{t-1} if Δp_t = 0
              { sign(Δp_t) if Δp_t ≠ 0
        """
        # Use the vectorized Numba function
        prices = df['price'].values.astype(np.float64)
        tick_rules = apply_tick_rule_vectorized(
            prices,
            previous_price if previous_price is not None else prices[0],
            previous_tick_rule
        )
        
        return pd.Series(tick_rules, index=df.index, dtype=np.int64)

    def _calculate_ewma_alpha(self, span: int) -> float:
        """Calculate EWMA alpha factor from span"""
        return calculate_ewma_alpha(span)

    def _update_ewma(self, alpha: float, old_value: Optional[float], 
                     new_value: float) -> float:
        """Update value using Exponentially Weighted Moving Average"""
        if old_value is None:
            return new_value
        return update_ewma(alpha, old_value, new_value)

    def _init_bar(self, row: pd.Series) -> Dict[str, Any]:
        """Initialize a new bar"""
        return {
            'open_time': row['timestamp'],
            'close_time': row['timestamp'],
            'open': row['price'],
            'high': row['price'],
            'low': row['price'],
            'close': row['price'],
            'volume': row['volume'],
            'dollar_volume': row['dollar_volume'],
            'tick_count': 1,
            'buy_volume': row['volume'] if row['tick_rule'] > 0 else 0,
            'buy_tick_count': 1 if row['tick_rule'] > 0 else 0,
            'vwap_numerator': row['price'] * row['volume'],
            'vwap_denominator': row['volume']
        }

    def _update_bar(self, bar: Dict[str, Any], row: pd.Series):
        """Update bar with new tick using optimized function"""
        # Use Numba-optimized function for performance
        (bar['high'], bar['low'], bar['volume'], bar['dollar_volume'],
         bar['tick_count'], bar['buy_volume'], bar['buy_tick_count'],
         bar['vwap_numerator']) = update_bar_numba(
            bar['open'], bar['high'], bar['low'], bar['volume'],
            bar['dollar_volume'], bar['tick_count'], bar['buy_volume'],
            bar['buy_tick_count'], bar['vwap_numerator'],
            row['price'], row['volume'], row['dollar_volume'], row['tick_rule']
        )
        
        bar['close'] = row['price']
        bar['close_time'] = row['timestamp']
        
        bar['vwap_denominator'] += row['volume']

    def _update_bar_stats(self, stats: np.ndarray, row: pd.Series):
        """Update bar statistics for EWMA calculations using optimized function"""
        update_bar_stats_numba(stats, row['volume'], row['dollar_volume'], row['tick_rule'])

    def _finalize_bar(self, bar: Dict[str, Any], threshold: float, 
                      bar_length: int, theta_t: Optional[float] = None) -> bool:
        """
        Finalize bar with calculated metrics
        theta_t: valor acumulado para imbalance bars
        """
        # Check minimum bar length requirement
        min_bar_length = self.parameters.get('min_bar_length', 1)
        if bar_length < min_bar_length:
            self.log_progress(
                f"Bar has only {bar_length} ticks, waiting for minimum {min_bar_length}",
                status="info"
            )
            return False
        
        # Calculate VWAP
        bar['vwap'] = (bar['vwap_numerator'] / bar['vwap_denominator'] 
                       if bar['vwap_denominator'] > 0 else bar['close'])
        
        # Calculate imbalance (buy_volume - sell_volume) / total_volume
        # sell_volume = total_volume - buy_volume
        sell_volume = bar['volume'] - bar['buy_volume']
        bar['imbalance'] = (bar['buy_volume'] - sell_volume) / bar['volume'] if bar['volume'] > 0 else 0
        
        # NÃO adicionar sell_volume ao bar final
        
        # Add metadata
        bar['bar_type'] = self.bar_type.value
        bar['threshold_value'] = threshold
        
        # Adicionar campos específicos baseado no tipo de bar
        if self.bar_type in [BarType.TICK, BarType.TICK_IMBALANCE, BarType.TICK_RUNS]:
            bar['expected_bar_length'] = self.ewma_state.T if self.ewma_state.T else threshold
            bar['actual_bar_length'] = bar_length
        elif self.bar_type in [BarType.VOLUME, BarType.VOLUME_IMBALANCE, BarType.VOLUME_RUNS]:
            bar['expected_bar_volume'] = self.ewma_state.T if self.ewma_state.T else threshold
            bar['actual_bar_volume'] = bar['volume']
        elif self.bar_type in [BarType.DOLLAR, BarType.DOLLAR_IMBALANCE, BarType.DOLLAR_RUNS]:
            bar['expected_bar_dollar_volume'] = self.ewma_state.T if self.ewma_state.T else threshold
            bar['actual_bar_dollar_volume'] = bar['dollar_volume']
        
        # Adicionar campos específicos para imbalance bars
        if 'imbalance' in self.bar_type.value and theta_t is not None:
            bar['theta_t'] = theta_t
            bar['expected_T'] = self.ewma_state.T if self.ewma_state.T else threshold
            
            # Calcular expected_imbalance baseado no tipo
            if self.bar_type == BarType.TICK_IMBALANCE:
                # E[θ_T]/E[T] = |2P[b_t=1] - 1|
                bar['expected_imbalance'] = abs(self.ewma_state.expected_b_t)
            else:
                # E[θ_T]/E[T] = |2v+ - E[v_t]| = |expected_signed_volume|
                bar['expected_imbalance'] = abs(self.ewma_state.expected_signed_volume)
        
        # Remove calculation helpers
        if 'vwap_numerator' in bar:
            del bar['vwap_numerator']
        if 'vwap_denominator' in bar:
            del bar['vwap_denominator']
        
        return True

    def _save_bars_batch(self, bars_list: List[Dict], final: bool = False):
        """Save a batch of bars incrementally with optimized I/O"""
        if not bars_list:
            return
        
        df = pd.DataFrame(bars_list)
        
        # Prepare filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if final and self.batch_number == 0:
            # If it's the only batch, use simple name
            filename = f"bars_{self.bar_type.value}_{timestamp}.parquet"
        else:
            # Multiple batches
            filename = f"bars_{self.bar_type.value}_{timestamp}_part{self.batch_number:04d}.parquet"
        
        output_file = self.temp_dir / filename
        
        # Ensure correct data types using vectorized operations
        float_cols = ['open', 'high', 'low', 'close', 'volume', 'dollar_volume', 
                     'vwap', 'buy_volume', 'imbalance', 
                     'threshold_value']
        
        # Adicionar colunas específicas do tipo de bar
        if 'expected_bar_length' in df.columns:
            float_cols.append('expected_bar_length')
        if 'expected_bar_volume' in df.columns:
            float_cols.append('expected_bar_volume')
        if 'expected_bar_dollar_volume' in df.columns:
            float_cols.append('expected_bar_dollar_volume')
        if 'actual_bar_volume' in df.columns:
            float_cols.append('actual_bar_volume')
        if 'actual_bar_dollar_volume' in df.columns:
            float_cols.append('actual_bar_dollar_volume')
        if 'theta_t' in df.columns:
            float_cols.append('theta_t')
        if 'expected_imbalance' in df.columns:
            float_cols.append('expected_imbalance')
        if 'expected_T' in df.columns:
            float_cols.append('expected_T')
        
        int_cols = ['tick_count', 'buy_tick_count']
        if 'actual_bar_length' in df.columns:
            int_cols.append('actual_bar_length')
        
        # Vectorized type conversion
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].astype(np.float64)
        
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(np.int64)
        
        # Convert to PyArrow Table and save with compression
        table = pa.Table.from_pandas(df, schema=self.output_schema)
        pq.write_table(table, output_file, compression='snappy', use_dictionary=True)
        
        self.batch_number += 1
        self.log_progress(f"Saved batch {self.batch_number} with {len(bars_list)} bars", status="info")

    def _merge_bar_files(self) -> Path:
        """Merge all bar files into a single file with optimized I/O"""
        bar_files = sorted(self.temp_dir.glob(f"bars_{self.bar_type.value}_*_part*.parquet"))
        
        if not bar_files:
            # Check if there's a single file without part suffix
            single_files = list(self.temp_dir.glob(f"bars_{self.bar_type.value}_*.parquet"))
            if single_files:
                # Move the single file to output directory
                final_file = self.output_dir / single_files[0].name
                shutil.move(str(single_files[0]), str(final_file))
                return final_file
            return None
        
        # Name for final file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_file = self.output_dir / f"bars_{self.bar_type.value}_{timestamp}.parquet"
        
        self.log_progress(f"Merging {len(bar_files)} batch files...", 95)
        
        # Use PyArrow for efficient merge with memory mapping
        tables = []
        for file in bar_files:
            table = pq.read_table(file, memory_map=True)
            tables.append(table)
            
        # Concatenate all tables
        combined_table = pa.concat_tables(tables)
        pq.write_table(combined_table, final_file, compression='snappy', use_dictionary=True)
        
        # Clean up temporary files
        for file in bar_files:
            file.unlink()
        
        self.log_progress(f"Merged into {final_file.name}", status="info")
        return final_file

    def _process_chunk_parallel(self, chunk_data: Tuple[int, pd.DataFrame], 
                               bar_type: BarType, parameters: Dict) -> Tuple[List[Dict], Dict]:
        """Process a chunk in parallel (for multiprocessing)"""
        chunk_id, chunk = chunk_data
        bars = []
        stats = {}
        
        # Process according to bar type
        # This is a simplified version - in practice, you'd need to handle state properly
        # For now, we'll use parallel processing only for initial tick rule application
        return bars, stats

    def process_with_dask(self) -> Dict[str, Any]:
        """Process bars using Dask for distributed computing"""
        if not self.use_dask:
            self.log_progress("Dask not available or disabled, falling back to standard processing", status="warning")
            return self._process_standard_fallback()
        
        try:
            # Load data with Dask
            ddf = self.load_data_with_dask()
            
            # Apply tick rule to all partitions
            self.log_progress("Applying tick rule across all partitions...")
            
            # Get initial values
            first_row = ddf.head(1)
            init_price = first_row['price'].values[0]
            init_tick_rule = 1
            
            # Apply tick rule to each partition sequentially to maintain state
            partitions = []
            prev_price = init_price
            prev_tick_rule = init_tick_rule
            
            for i in range(ddf.npartitions):
                partition = ddf.get_partition(i)
                
                # Apply tick rule with previous values
                partition = partition.map_partitions(
                    self._preprocess_dask_partition,
                    prev_price,
                    prev_tick_rule,
                    meta=partition._meta
                )
                
                # Get last values for next partition
                last_row = partition.tail(1).compute()
                if len(last_row) > 0:
                    prev_price = last_row['price'].values[0]
                    prev_tick_rule = last_row['tick_rule'].values[0]
                
                partitions.append(partition)
            
            # Concatenate all processed partitions
            ddf = dd.concat(partitions)
            
            # Route to appropriate processor based on bar type
            if self.bar_type in [BarType.TICK, BarType.VOLUME, BarType.DOLLAR]:
                return self._process_standard_bars_dask(ddf)
            elif 'imbalance' in self.bar_type.value:
                return self._process_imbalance_bars_dask(ddf)
            elif 'runs' in self.bar_type.value:
                return self._process_run_bars_dask(ddf)
            else:
                raise ValueError(f"Unsupported bar type for Dask: {self.bar_type}")
                
        except Exception as e:
            self.log_progress(f"Error in Dask processing: {e}. Falling back to standard processing.", status="warning")
            self.use_dask = False
            return self._process_standard_fallback()

    def _process_standard_bars_dask(self, ddf: dd.DataFrame) -> Dict[str, Any]:
        """Process standard bars using Dask"""
        # Get threshold
        threshold_map = {
            BarType.TICK: 'tick_threshold',
            BarType.VOLUME: 'volume_threshold',
            BarType.DOLLAR: 'dollar_threshold'
        }
        threshold = self.parameters[threshold_map[self.bar_type]]
        
        self.log_progress(f"Processing {self.bar_type.value} bars with Dask (threshold: {threshold})...")
        
        # Process each partition and collect bars
        futures = []
        
        for i in range(ddf.npartitions):
            partition = ddf.get_partition(i)
            future = self.dask_client.submit(
                self._process_standard_partition,
                partition,
                self.bar_type,
                threshold,
                self.chunk_buffer.accumulator if i == 0 else 0,
                self.chunk_buffer.bar_start_idx if i == 0 else 0
            )
            futures.append(future)
        
        # Process results as they complete
        bars_collected = []
        
        for future in as_completed(futures):
            try:
                bars, final_state = future.result()
                bars_collected.extend(bars)
                
                # Update state from last partition
                self.chunk_buffer.accumulator = final_state['accumulator']
                self.chunk_buffer.processed_rows = final_state['processed_rows']
                
                # Save batch if needed
                if len(bars_collected) >= self.bars_per_batch:
                    self._save_bars_batch(bars_collected[:self.bars_per_batch])
                    bars_collected = bars_collected[self.bars_per_batch:]
                    self.total_bars_generated += self.bars_per_batch
                    self._check_memory_usage()
                    
            except Exception as e:
                self.log_progress(f"Error processing partition: {e}", status="warning")
        
        # Save remaining bars
        if bars_collected:
            self._save_bars_batch(bars_collected, final=True)
            self.total_bars_generated += len(bars_collected)
        
        return {"total_bars": self.total_bars_generated}

    def _process_standard_partition(self, partition_future, bar_type: BarType, 
                                   threshold: float, init_accumulator: float,
                                   init_bar_start_idx: int) -> Tuple[List[Dict], Dict]:
        """Process a single partition for standard bars"""
        # Get partition data
        partition = partition_future.compute()
        
        bars = []
        current_bar = None
        accumulator = init_accumulator
        bar_start_idx = init_bar_start_idx
        processed_rows = init_bar_start_idx
        
        # Convert to numpy arrays
        timestamps = partition['timestamp'].values
        prices = partition['price'].values.astype(np.float64)
        volumes = partition['volume'].values.astype(np.float64)
        dollar_volumes = partition['dollar_volume'].values.astype(np.float64)
        tick_rules = partition['tick_rule'].values.astype(np.int64)
        
        # Use vectorized processing where possible
        if bar_type == BarType.TICK:
            # Find bar boundaries
            boundaries, processed_rows = process_tick_bars_vectorized(
                timestamps, prices, volumes, dollar_volumes, tick_rules,
                int(threshold), bar_start_idx
            )
            
            # Extract bars at boundaries
            start = 0
            for end in boundaries:
                if end > start:
                    bar = self._create_bar_from_slice(
                        timestamps[start:end],
                        prices[start:end],
                        volumes[start:end],
                        dollar_volumes[start:end],
                        tick_rules[start:end],
                        threshold
                    )
                    bars.append(bar)
                    start = end
                    
        else:
            # For volume/dollar bars, process sequentially for now
            for i in range(len(partition)):
                # Similar logic to original process_standard_bars
                # but without state persistence between chunks
                pass
        
        final_state = {
            'accumulator': accumulator,
            'processed_rows': processed_rows
        }
        
        return bars, final_state

    def _create_bar_from_slice(self, timestamps, prices, volumes, 
                              dollar_volumes, tick_rules, threshold) -> Dict:
        """Create a bar from numpy array slices"""
        # Check if we have any data
        if len(prices) == 0:
            # Return empty bar for empty data
            return {
                'open_time': pd.Timestamp.now(),
                'close_time': pd.Timestamp.now(),
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': 0.0,
                'dollar_volume': 0.0,
                'tick_count': 0,
                'vwap': 0.0,
                'buy_volume': 0.0,
                'buy_tick_count': 0,
                'imbalance': 0.0,
                'bar_type': self.bar_type.value,
                'threshold_value': threshold,
            }
        
        buy_mask = tick_rules > 0
        sell_volume = volumes[~buy_mask].sum() if (~buy_mask).any() else 0
        
        bar = {
            'open_time': pd.Timestamp(timestamps[0]),
            'close_time': pd.Timestamp(timestamps[-1]),
            'open': prices[0],
            'high': prices.max(),
            'low': prices.min(),
            'close': prices[-1],
            'volume': volumes.sum(),
            'dollar_volume': dollar_volumes.sum(),
            'tick_count': len(prices),
            'buy_volume': volumes[buy_mask].sum() if buy_mask.any() else 0,
            'buy_tick_count': buy_mask.sum(),
            'vwap': (prices * volumes).sum() / volumes.sum() if volumes.sum() > 0 else prices[-1],
            'imbalance': 0,  # Will be calculated in finalize
            'bar_type': self.bar_type.value,
            'threshold_value': threshold,
        }
        
        # Calculate imbalance
        if bar['volume'] > 0:
            bar['imbalance'] = (bar['buy_volume'] - (bar['volume'] - bar['buy_volume'])) / bar['volume']
        
        # Add specific fields based on bar type
        if self.bar_type in [BarType.TICK, BarType.TICK_IMBALANCE, BarType.TICK_RUNS]:
            bar['expected_bar_length'] = self.ewma_state.T if self.ewma_state.T else threshold
            bar['actual_bar_length'] = len(prices)
        elif self.bar_type in [BarType.VOLUME, BarType.VOLUME_IMBALANCE, BarType.VOLUME_RUNS]:
            bar['expected_bar_volume'] = self.ewma_state.T if self.ewma_state.T else threshold
            bar['actual_bar_volume'] = bar['volume']
        elif self.bar_type in [BarType.DOLLAR, BarType.DOLLAR_IMBALANCE, BarType.DOLLAR_RUNS]:
            bar['expected_bar_dollar_volume'] = self.ewma_state.T if self.ewma_state.T else threshold
            bar['actual_bar_dollar_volume'] = bar['dollar_volume']
        
        return bar

    def _process_standard_fallback(self) -> Dict[str, Any]:
        """Fallback to standard processing when Dask fails"""
        if self.bar_type in [BarType.TICK, BarType.VOLUME, BarType.DOLLAR]:
            return self.process_standard_bars()
        elif 'imbalance' in self.bar_type.value:
            return self.process_imbalance_bars()
        elif 'runs' in self.bar_type.value:
            return self.process_run_bars()
        else:
            raise ValueError(f"Unsupported bar type: {self.bar_type}")

    def _process_imbalance_bars_dask(self, ddf: dd.DataFrame) -> Dict[str, Any]:
        """Process imbalance bars using Dask (placeholder for now)"""
        # Due to the sequential nature of imbalance bars and EWMA updates,
        # Dask processing would require significant refactoring
        # For now, fall back to standard processing
        self.log_progress("Imbalance bars not yet optimized for Dask, using standard processing", status="info")
        return self.process_imbalance_bars()

    def _process_run_bars_dask(self, ddf: dd.DataFrame) -> Dict[str, Any]:
        """Process run bars using Dask (placeholder for now)"""
        # Due to the sequential nature of run bars and EWMA updates,
        # Dask processing would require significant refactoring
        # For now, fall back to standard processing
        self.log_progress("Run bars not yet optimized for Dask, using standard processing", status="info")
        return self.process_run_bars()

    def process_standard_bars(self) -> Dict[str, Any]:
        """
        Process standard bars (tick, volume, dollar) with memory optimization
        
        Standard bars use fixed thresholds:
        - Tick bars: Close after fixed number of ticks
        - Volume bars: Close after fixed volume accumulated
        - Dollar bars: Close after fixed dollar volume accumulated
        """
        # Use instance variable for bars that persist across chunks
        
        # Get threshold based on bar type
        threshold_map = {
            BarType.TICK: 'tick_threshold',
            BarType.VOLUME: 'volume_threshold',
            BarType.DOLLAR: 'dollar_threshold'
        }
        threshold = self.parameters[threshold_map[self.bar_type]]
        
        # Get optional parameters
        min_bar_length = self.parameters.get('min_bar_length', 1)
        use_adaptive_threshold = self.parameters.get('use_adaptive_threshold', False)
        adaptive_factor = self.parameters.get('adaptive_factor', 0.1)  # How much to adapt
        
        # Initialize from buffer
        current_bar = self.chunk_buffer.current_bar
        accumulator = self.chunk_buffer.accumulator
        bar_start_idx = self.chunk_buffer.bar_start_idx
        bar_stats = self.chunk_buffer.current_bar_stats
        
        # For adaptive threshold tracking
        if use_adaptive_threshold and self._is_information_driven():
            ewma_span = self.parameters.get('ewma_span', 100)
            alpha = self._calculate_ewma_alpha(ewma_span)
        
        # Process chunks
        total_chunks = self._estimate_total_chunks()
        processed_chunks = 0
        
        for chunk_id, chunk in self.load_data_chunked():
            processed_chunks += 1
            chunk_progress = processed_chunks / total_chunks
            base_progress = 20 + chunk_progress * 60
            
            self.log_progress(
                f"Processing {self.bar_type.value} bars - chunk {processed_chunks}/{total_chunks}... "
                f"{self.total_bars_generated} bars generated",
                base_progress
            )
            
            # Convert to numpy arrays for faster processing
            timestamps = chunk['timestamp'].values
            prices = chunk['price'].values.astype(np.float64)
            volumes = chunk['volume'].values.astype(np.float64)
            dollar_volumes = chunk['dollar_volume'].values.astype(np.float64)
            tick_rules = chunk['tick_rule'].values.astype(np.int64)
            
            for i in range(len(chunk)):
                self.pause_event.wait()
                if self.cancelled:
                    raise Exception("Processing cancelled")
                
                # Initialize new bar if needed
                if current_bar is None:
                    current_bar = {
                        'open_time': timestamps[i],
                        'close_time': timestamps[i],
                        'open': prices[i],
                        'high': prices[i],
                        'low': prices[i],
                        'close': prices[i],
                        'volume': volumes[i],
                        'dollar_volume': dollar_volumes[i],
                        'tick_count': 1,
                        'buy_volume': volumes[i] if tick_rules[i] > 0 else 0,
                        'buy_tick_count': 1 if tick_rules[i] > 0 else 0,
                        'vwap_numerator': prices[i] * volumes[i],
                        'vwap_denominator': volumes[i]
                    }
                    bar_start_idx = self.chunk_buffer.processed_rows
                    bar_stats = self.chunk_buffer._init_bar_stats()
                    
                    # Reset accumulator for new bar
                    if self.bar_type == BarType.TICK:
                        accumulator = 1
                    elif self.bar_type == BarType.VOLUME:
                        accumulator = volumes[i]
                    elif self.bar_type == BarType.DOLLAR:
                        accumulator = dollar_volumes[i]
                else:
                    # Update bar using optimized function
                    (current_bar['high'], current_bar['low'], current_bar['volume'], 
                    current_bar['dollar_volume'], current_bar['tick_count'], 
                    current_bar['buy_volume'], current_bar['buy_tick_count'],
                    current_bar['vwap_numerator']) = update_bar_numba(
                        current_bar['open'], current_bar['high'], current_bar['low'],
                        current_bar['volume'], current_bar['dollar_volume'],
                        current_bar['tick_count'], current_bar['buy_volume'],
                        current_bar['buy_tick_count'], current_bar['vwap_numerator'],
                        prices[i], volumes[i], dollar_volumes[i], tick_rules[i]
                    )
                    
                    current_bar['close'] = prices[i]
                    current_bar['close_time'] = timestamps[i]
                    
                    current_bar['vwap_denominator'] += volumes[i]
                    
                    # Update accumulator based on bar type
                    if self.bar_type == BarType.TICK:
                        accumulator += 1
                    elif self.bar_type == BarType.VOLUME:
                        accumulator += volumes[i]
                    elif self.bar_type == BarType.DOLLAR:
                        accumulator += dollar_volumes[i]
                
                # Update statistics
                update_bar_stats_numba(bar_stats, volumes[i], dollar_volumes[i], tick_rules[i])
                
                self.chunk_buffer.processed_rows += 1
                
                # Calculate current bar length
                bar_length = self.chunk_buffer.processed_rows - bar_start_idx
                
                # Determine effective threshold (with optional adaptation)
                effective_threshold = threshold
                if use_adaptive_threshold and self.ewma_state.T is not None:
                    # Blend fixed threshold with adaptive component
                    adaptive_component = self.ewma_state.T * adaptive_factor
                    effective_threshold = threshold * (1 - adaptive_factor) + adaptive_component
                
                # Check if bar is complete
                if accumulator >= effective_threshold and bar_length >= min_bar_length:
                    # Try to finalize the bar
                    if self._finalize_bar(current_bar, effective_threshold, bar_length):
                        self.pending_bars.append(current_bar)
                        self.total_bars_generated += 1
                        
                        # Log every N bars for monitoring
                        if self.total_bars_generated % 1000 == 0:
                            self.log_progress(
                                f"Generated {self.total_bars_generated} bars. "
                                f"Last bar: {bar_length} ticks, accumulator={accumulator:.2f}",
                                status="info"
                            )
                        
                        # Save batch if needed
                        if len(self.pending_bars) >= self.bars_per_batch:
                            self._save_bars_batch(self.pending_bars)
                            self.pending_bars = []
                            self._check_memory_usage()
                        
                        # Update EWMA if using adaptive threshold or information-driven
                        if use_adaptive_threshold or self._is_information_driven():
                            self._update_ewma_after_bar(bar_length, bar_stats)
                        
                        # Reset for next bar
                        current_bar = None
                        accumulator = 0
                        bar_stats = self.chunk_buffer._init_bar_stats()
                    else:
                        # Bar didn't meet minimum requirements, continue accumulating
                        # This shouldn't happen often with standard bars
                        self.log_progress(
                            f"Bar ready but not finalized: length={bar_length}, min={min_bar_length}",
                            status="warning"
                        )
                
                # Safety check: prevent bars from growing too large
                elif self.bar_type == BarType.TICK and bar_length > threshold * 2:
                    # For tick bars, force close if we have 2x the expected ticks
                    self.log_progress(
                        f"Force closing tick bar after {bar_length} ticks (2x threshold)",
                        status="warning"
                    )
                    
                    if self._finalize_bar(current_bar, effective_threshold, bar_length):
                        current_bar['oversized'] = True  # Mark as oversized
                        self.pending_bars.append(current_bar)
                        self.total_bars_generated += 1
                        
                        if len(self.pending_bars) >= self.bars_per_batch:
                            self._save_bars_batch(self.pending_bars)
                            self.pending_bars = []
                            self._check_memory_usage()
                    
                    # Reset regardless
                    current_bar = None
                    accumulator = 0
                    bar_stats = self.chunk_buffer._init_bar_stats()
                
                elif (self.bar_type in [BarType.VOLUME, BarType.DOLLAR] and 
                    bar_length > self.parameters.get('max_bar_length', 100000)):
                    # For volume/dollar bars, use a configurable max length
                    self.log_progress(
                        f"Force closing {self.bar_type.value} bar after {bar_length} ticks (max length)",
                        status="warning"
                    )
                    
                    if self._finalize_bar(current_bar, effective_threshold, bar_length):
                        current_bar['oversized'] = True
                        self.pending_bars.append(current_bar)
                        self.total_bars_generated += 1
                        
                        if len(self.pending_bars) >= self.bars_per_batch:
                            self._save_bars_batch(self.pending_bars)
                            self.pending_bars = []
                            self._check_memory_usage()
                    
                    # Reset
                    current_bar = None
                    accumulator = 0
                    bar_stats = self.chunk_buffer._init_bar_stats()
            
            # Update buffer state
            self.chunk_buffer.current_bar = current_bar
            self.chunk_buffer.accumulator = accumulator
            self.chunk_buffer.bar_start_idx = bar_start_idx
            self.chunk_buffer.current_bar_stats = bar_stats
            self._save_state()
            
            # Log chunk completion with more detail
            if processed_chunks % 5 == 0 or processed_chunks == total_chunks:
                if current_bar is not None:
                    current_bar_length = self.chunk_buffer.processed_rows - bar_start_idx
                    self.log_progress(
                        f"Chunk {processed_chunks}/{total_chunks} complete. "
                        f"Current bar progress: {accumulator:.2f}/{effective_threshold:.2f} "
                        f"({current_bar_length} ticks)",
                        status="info"
                    )
            
            gc.collect()
        
        # Handle incomplete bar at the end
        if current_bar is not None:
            bar_length = self.chunk_buffer.processed_rows - bar_start_idx
            
            # Decide whether to save incomplete bar
            save_incomplete = self.parameters.get('save_incomplete_bars', True)
            min_incomplete_size = self.parameters.get('min_incomplete_bar_size', 10)
            
            if save_incomplete and bar_length >= min_incomplete_size:
                # Calculate what percentage of threshold was reached
                completion_ratio = accumulator / threshold
                
                self.log_progress(
                    f"Finalizing incomplete bar: {bar_length} ticks, "
                    f"{completion_ratio:.1%} of threshold reached",
                    status="info"
                )
                
                if self._finalize_bar(current_bar, threshold, bar_length):
                    current_bar['incomplete'] = True
                    current_bar['completion_ratio'] = completion_ratio
                    self.pending_bars.append(current_bar)
                    self.total_bars_generated += 1
            else:
                self.log_progress(
                    f"Discarding incomplete bar with {bar_length} ticks "
                    f"(accumulator: {accumulator:.2f}/{threshold:.2f})",
                    status="info"
                )
        
        # Save final batch - ALWAYS save pending bars even if not a full batch
        if self.pending_bars:
            self._save_bars_batch(self.pending_bars, final=True)
            self.pending_bars = []
        
        # Final summary
        self.log_progress(
            f"Standard bars processing complete. Total bars: {self.total_bars_generated}, "
            f"Total ticks processed: {self.chunk_buffer.processed_rows}",
            status="info"
        )
        
        # Calculate and log efficiency metrics
        if self.total_bars_generated > 0:
            avg_ticks_per_bar = self.chunk_buffer.processed_rows / self.total_bars_generated
            efficiency = avg_ticks_per_bar / threshold if self.bar_type == BarType.TICK else 1.0
            
            self.log_progress(
                f"Efficiency metrics: Avg ticks/bar: {avg_ticks_per_bar:.1f}, "
                f"Efficiency: {efficiency:.2%}",
                status="info"
            )
        
        return {"total_bars": self.total_bars_generated}

    def _save_bars_from_buffer(self, bar_buffer: np.ndarray, theta_values: Optional[np.ndarray] = None):
        """
        Converte buffer NumPy para DataFrame e salva
        """
        # Colunas base (sem sell_volume)
        columns = [
            'open_time', 'close_time', 'open', 'high', 'low', 'close',
            'volume', 'dollar_volume', 'tick_count', 'vwap',
            'buy_volume', 'buy_tick_count', 'imbalance',
            'bar_type_id', 'threshold_value'
        ]
        
        # Cria DataFrame
        df = pd.DataFrame(bar_buffer[:, :len(columns)], columns=columns)
        
        # Converte timestamps
        df['open_time'] = pd.to_datetime(df['open_time'].astype(np.int64))
        df['close_time'] = pd.to_datetime(df['close_time'].astype(np.int64))
        
        # Converte bar_type_id para string
        df['bar_type'] = self.bar_type.value
        df.drop('bar_type_id', axis=1, inplace=True)
        
        # Adicionar campos específicos do tipo de bar
        if self.bar_type in [BarType.TICK, BarType.TICK_IMBALANCE, BarType.TICK_RUNS]:
            df['expected_bar_length'] = self.ewma_state.T
            df['actual_bar_length'] = df['tick_count']
        elif self.bar_type in [BarType.VOLUME, BarType.VOLUME_IMBALANCE, BarType.VOLUME_RUNS]:
            df['expected_bar_volume'] = self.ewma_state.T
            df['actual_bar_volume'] = df['volume']
        elif self.bar_type in [BarType.DOLLAR, BarType.DOLLAR_IMBALANCE, BarType.DOLLAR_RUNS]:
            df['expected_bar_dollar_volume'] = self.ewma_state.T
            df['actual_bar_dollar_volume'] = df['dollar_volume']
        
        # Adicionar campos de imbalance se fornecidos
        if 'imbalance' in self.bar_type.value:
            if theta_values is not None:
                df['theta_t'] = theta_values
            df['expected_T'] = self.ewma_state.T
            
            # Calcular expected_imbalance baseado no estado atual
            if self.bar_type == BarType.TICK_IMBALANCE:
                df['expected_imbalance'] = abs(self.ewma_state.expected_b_t)
            else:
                df['expected_imbalance'] = abs(self.ewma_state.expected_signed_volume)
        
        # Converte para tipos corretos
        int_columns = ['tick_count', 'buy_tick_count']
        if 'actual_bar_length' in df.columns:
            int_columns.append('actual_bar_length')
        
        for col in int_columns:
            if col in df.columns:
                df[col] = df[col].astype(np.int64)
        
        # Adiciona ao buffer de bars pendentes
        self.pending_bars.extend(df.to_dict('records'))
        
        # Salva se exceder limite
        if len(self.pending_bars) >= self.bars_per_batch:
            self._save_bars_batch(self.pending_bars[:self.bars_per_batch])
            self.pending_bars = self.pending_bars[self.bars_per_batch:]

    def process_imbalance_bars(self) -> Dict[str, Any]:
        """
        Versão otimizada do processamento de Imbalance Bars
        """
        # Mapeia bar type para ID numérico para uso em funções Numba
        bar_type_map = {
            BarType.TICK_IMBALANCE: 0,
            BarType.VOLUME_IMBALANCE: 1,
            BarType.DOLLAR_IMBALANCE: 2
        }
        bar_type_id = bar_type_map[self.bar_type]
        
        # Configurações otimizadas
        batch_size = self.parameters.get('imbalance_batch_size', 100)
        
        # Get EWMA spans
        ewma_span_T = self.parameters.get('ewma_span_T', 100)
        ewma_span_imbalance = self.parameters.get('ewma_span_imbalance', 100)
        alpha_T = self._calculate_ewma_alpha(ewma_span_T)
        alpha_imbalance = self._calculate_ewma_alpha(ewma_span_imbalance)
        
        warm_up_bars = self.parameters.get('warm_up_bars', 20)
        min_bar_length = self.parameters.get('min_bar_length', 1)
        
        # Inicializa estruturas otimizadas
        lazy_updater = EWMALazyUpdater(
            self.ewma_state, 
            alpha_T, 
            alpha_imbalance,
            update_frequency=self.parameters.get('ewma_update_frequency', 20)
        )
        threshold_cache = ThresholdCache(cache_size=5000)
        
        # Buffer para armazenar valores de theta_t para cada bar
        theta_values = []
        
        # Inicializa do buffer
        current_bar = self.chunk_buffer.current_bar
        current_bar_start = self.chunk_buffer.bar_start_idx
        theta_t = self.chunk_buffer.theta_t
        bar_stats = self.chunk_buffer.current_bar_stats
        
        # Process chunks
        total_chunks = self._estimate_total_chunks()
        processed_chunks = 0
        
        for chunk_id, chunk in self.load_data_chunked():
            processed_chunks += 1
            chunk_progress = processed_chunks / total_chunks
            base_progress = 20 + chunk_progress * 60
            
            self.log_progress(
                f"Processing {self.bar_type.value} bars (optimized) - "
                f"chunk {processed_chunks}/{total_chunks}... "
                f"{self.total_bars_generated} bars generated",
                base_progress
            )
            
            # Converte para arrays NumPy uma vez
            timestamps = chunk['timestamp'].values
            prices = chunk['price'].values.astype(np.float64)
            volumes = chunk['volume'].values.astype(np.float64)
            dollar_volumes = chunk['dollar_volume'].values.astype(np.float64)
            tick_rules = chunk['tick_rule'].values.astype(np.int64)
            
            chunk_size = len(chunk)
            i = 0
            
            while i < chunk_size:
                self.pause_event.wait()
                if self.cancelled:
                    raise Exception("Processing cancelled")
                
                # Initialize new bar if needed
                if current_bar is None:
                    current_bar = {
                        'open_time': timestamps[i],
                        'close_time': timestamps[i],
                        'open': prices[i],
                        'high': prices[i],
                        'low': prices[i],
                        'close': prices[i],
                        'volume': volumes[i],
                        'dollar_volume': dollar_volumes[i],
                        'tick_count': 1,
                        'buy_volume': volumes[i] if tick_rules[i] > 0 else 0,
                        'buy_tick_count': 1 if tick_rules[i] > 0 else 0,
                        'vwap_numerator': prices[i] * volumes[i],
                        'vwap_denominator': volumes[i]
                    }
                    current_bar_start = self.chunk_buffer.processed_rows
                    bar_stats = self.chunk_buffer._init_bar_stats()
                    
                    # Initialize theta
                    if bar_type_id == 0:  # TICK_IMBALANCE
                        theta_t = tick_rules[i]
                    elif bar_type_id == 1:  # VOLUME_IMBALANCE
                        theta_t = tick_rules[i] * volumes[i]
                    else:  # DOLLAR_IMBALANCE
                        theta_t = tick_rules[i] * dollar_volumes[i]
                    
                    # Update statistics
                    update_bar_stats_numba(bar_stats, volumes[i], dollar_volumes[i], tick_rules[i])
                    self.chunk_buffer.processed_rows += 1
                    i += 1
                    continue
                
                # Calcula threshold com cache
                if self.bar_type == BarType.TICK_IMBALANCE:
                    expected_imbalance = abs(self.ewma_state.expected_b_t)
                else:
                    expected_imbalance = abs(self.ewma_state.expected_signed_volume)
                
                threshold = threshold_cache.get_threshold(
                    self.ewma_state.T,
                    expected_imbalance,
                    self.total_bars_generated,
                    warm_up_bars,
                    self.bar_type
                )
                
                # Determina tamanho do batch para processar
                remaining = chunk_size - i
                current_batch_size = min(batch_size, remaining)
                
                # Processa batch
                bar_end_idx, new_theta = process_imbalance_batch(
                    prices[i:i+current_batch_size],
                    volumes[i:i+current_batch_size],
                    dollar_volumes[i:i+current_batch_size],
                    tick_rules[i:i+current_batch_size],
                    theta_t,
                    threshold,
                    bar_type_id,
                    min(10, current_batch_size)  # micro-batch size
                )
                
                if bar_end_idx >= 0:
                    # Bar completo encontrado
                    actual_end = i + bar_end_idx + 1
                    bar_length = actual_end + self.chunk_buffer.processed_rows - current_bar_start - i
                    
                    # Atualiza o bar existente com os dados restantes
                    for j in range(i, actual_end):
                        (current_bar['high'], current_bar['low'], current_bar['volume'], 
                        current_bar['dollar_volume'], current_bar['tick_count'], 
                        current_bar['buy_volume'], current_bar['buy_tick_count'],
                        current_bar['vwap_numerator']) = update_bar_numba(
                            current_bar['open'], current_bar['high'], current_bar['low'],
                            current_bar['volume'], current_bar['dollar_volume'],
                            current_bar['tick_count'], current_bar['buy_volume'],
                            current_bar['buy_tick_count'], current_bar['vwap_numerator'],
                            prices[j], volumes[j], dollar_volumes[j], tick_rules[j]
                        )
                        current_bar['close'] = prices[j]
                        current_bar['close_time'] = timestamps[j]
                        current_bar['vwap_denominator'] += volumes[j]
                        update_bar_stats_numba(bar_stats, volumes[j], dollar_volumes[j], tick_rules[j])
                    
                    # Verifica comprimento mínimo
                    if bar_length >= min_bar_length:
                        # Finaliza o bar com theta_t
                        if self._finalize_bar(current_bar, threshold, bar_length, theta_t=new_theta):
                            self.pending_bars.append(current_bar)
                            self.total_bars_generated += 1
                            
                            # Atualiza EWMA com lazy update
                            lazy_updater.add_observation(bar_length, bar_stats, new_theta)
                            
                            # Log periódico
                            if self.total_bars_generated % 100 == 0:
                                self.log_progress(
                                    f"Generated {self.total_bars_generated} bars. "
                                    f"Last: θ={new_theta:.2f}, threshold={threshold:.2f}, "
                                    f"length={bar_length}",
                                    status="info"
                                )
                            
                            # Save batch if needed
                            if len(self.pending_bars) >= self.bars_per_batch:
                                self._save_bars_batch(self.pending_bars)
                                self.pending_bars = []
                                self._check_memory_usage()
                    
                    # Reset para próximo bar
                    current_bar = None
                    current_bar_start = self.chunk_buffer.processed_rows + actual_end
                    theta_t = 0.0
                    i = actual_end
                    bar_stats = self.chunk_buffer._init_bar_stats()
                    
                else:
                    # Nenhum bar completo no batch
                    # Atualiza o bar em progresso
                    for j in range(i, i + current_batch_size):
                        (current_bar['high'], current_bar['low'], current_bar['volume'], 
                        current_bar['dollar_volume'], current_bar['tick_count'], 
                        current_bar['buy_volume'], current_bar['buy_tick_count'],
                        current_bar['vwap_numerator']) = update_bar_numba(
                            current_bar['open'], current_bar['high'], current_bar['low'],
                            current_bar['volume'], current_bar['dollar_volume'],
                            current_bar['tick_count'], current_bar['buy_volume'],
                            current_bar['buy_tick_count'], current_bar['vwap_numerator'],
                            prices[j], volumes[j], dollar_volumes[j], tick_rules[j]
                        )
                        current_bar['close'] = prices[j]
                        current_bar['close_time'] = timestamps[j]
                        current_bar['vwap_denominator'] += volumes[j]
                        update_bar_stats_numba(bar_stats, volumes[j], dollar_volumes[j], tick_rules[j])
                    
                    theta_t = new_theta
                    i += current_batch_size
            
            # Atualiza estado do buffer
            self.chunk_buffer.processed_rows += chunk_size
            self.chunk_buffer.theta_t = theta_t
            self.chunk_buffer.bar_start_idx = current_bar_start
            self.chunk_buffer.current_bar = current_bar
            self.chunk_buffer.current_bar_stats = bar_stats
            self._save_state()
            
            gc.collect()
        
        # Força atualização final do EWMA
        lazy_updater.force_update()
        
        # Handle incomplete bar
        if current_bar is not None:
            bar_length = self.chunk_buffer.processed_rows - current_bar_start
            
            save_incomplete = self.parameters.get('save_incomplete_bars', True)
            min_incomplete_size = self.parameters.get('min_incomplete_bar_size', 10)
            
            if save_incomplete and bar_length >= min_incomplete_size:
                if self.bar_type == BarType.TICK_IMBALANCE:
                    expected_imbalance = abs(self.ewma_state.expected_b_t)
                else:
                    expected_imbalance = abs(self.ewma_state.expected_signed_volume)
                
                threshold = self.ewma_state.T * expected_imbalance
                
                if self._finalize_bar(current_bar, threshold, bar_length, theta_t=theta_t):
                    current_bar['incomplete'] = True
                    self.pending_bars.append(current_bar)
                    self.total_bars_generated += 1
        
        # Salva qualquer bar pendente
        if self.pending_bars:
            self._save_bars_batch(self.pending_bars, final=True)
            self.pending_bars = []
        
        # Estatísticas finais
        self.log_progress(
            f"Imbalance bars processing complete (optimized). "
            f"Total bars: {self.total_bars_generated}, "
            f"Total ticks: {self.chunk_buffer.processed_rows}, "
            f"Final E[T]: {self.ewma_state.T:.2f}",
            status="info"
        )
        
        return {"total_bars": self.total_bars_generated}

    def process_run_bars(self) -> Dict[str, Any]:
        """
        Process run bars (TRB, VRB, DRB) according to Lopez de Prado
        Formula: θ_T = max{Σ v_t where b_t=1, Σ v_t where b_t=-1}
        """
        # Use instance variable for bars that persist across chunks
        
        # Get appropriate EWMA spans
        if self.bar_type == BarType.TICK_RUNS:
            ewma_span_T = self.parameters.get('ewma_span_T', 100)
            ewma_span_probability = self.parameters.get('ewma_span_probability', 100)
            alpha_T = self._calculate_ewma_alpha(ewma_span_T)
            alpha_prob = self._calculate_ewma_alpha(ewma_span_probability)
        else:  # Volume/Dollar runs
            ewma_span_T = self.parameters.get('ewma_span_T', 100)
            ewma_span_volumes = self.parameters.get('ewma_span_volumes', 100)
            alpha_T = self._calculate_ewma_alpha(ewma_span_T)
            alpha_volumes = self._calculate_ewma_alpha(ewma_span_volumes)
        
        warm_up_bars = self.parameters.get('warm_up_bars', 20)
        
        # Initialize from buffer
        current_bar = self.chunk_buffer.current_bar
        buy_accumulator = self.chunk_buffer.buy_accumulator
        sell_accumulator = self.chunk_buffer.sell_accumulator
        bar_start_idx = self.chunk_buffer.bar_start_idx
        bar_stats = self.chunk_buffer.current_bar_stats
        
        # Process chunks
        total_chunks = self._estimate_total_chunks()
        processed_chunks = 0
        
        for chunk_id, chunk in self.load_data_chunked():
            processed_chunks += 1
            chunk_progress = processed_chunks / total_chunks
            base_progress = 20 + chunk_progress * 60
            
            self.log_progress(
                f"Processing {self.bar_type.value} bars - chunk {processed_chunks}/{total_chunks}... "
                f"{self.total_bars_generated} bars generated",
                base_progress
            )
            
            # Convert to numpy arrays for faster processing
            timestamps = chunk['timestamp'].values
            prices = chunk['price'].values.astype(np.float64)
            volumes = chunk['volume'].values.astype(np.float64)
            dollar_volumes = chunk['dollar_volume'].values.astype(np.float64)
            tick_rules = chunk['tick_rule'].values.astype(np.int64)
            
            for i in range(len(chunk)):
                self.pause_event.wait()
                if self.cancelled:
                    raise Exception("Processing cancelled")
                
                # Initialize new bar if needed
                if current_bar is None:
                    current_bar = {
                        'open_time': timestamps[i],
                        'close_time': timestamps[i],
                        'open': prices[i],
                        'high': prices[i],
                        'low': prices[i],
                        'close': prices[i],
                        'volume': volumes[i],
                        'dollar_volume': dollar_volumes[i],
                        'tick_count': 1,
                        'buy_volume': volumes[i] if tick_rules[i] > 0 else 0,
                        'buy_tick_count': 1 if tick_rules[i] > 0 else 0,
                        'vwap_numerator': prices[i] * volumes[i],
                        'vwap_denominator': volumes[i]
                    }
                    buy_accumulator = 0
                    sell_accumulator = 0
                    bar_start_idx = self.chunk_buffer.processed_rows
                    bar_stats = self.chunk_buffer._init_bar_stats()
                else:
                    # Update bar using optimized function
                    (current_bar['high'], current_bar['low'], current_bar['volume'], 
                     current_bar['dollar_volume'], current_bar['tick_count'], 
                     current_bar['buy_volume'], current_bar['buy_tick_count'],
                     current_bar['vwap_numerator']) = update_bar_numba(
                        current_bar['open'], current_bar['high'], current_bar['low'],
                        current_bar['volume'], current_bar['dollar_volume'],
                        current_bar['tick_count'], current_bar['buy_volume'],
                        current_bar['buy_tick_count'], current_bar['vwap_numerator'],
                        prices[i], volumes[i], dollar_volumes[i], tick_rules[i]
                    )
                    
                    current_bar['close'] = prices[i]
                    current_bar['close_time'] = timestamps[i]
                    
                    current_bar['vwap_denominator'] += volumes[i]
                
                # Update statistics
                update_bar_stats_numba(bar_stats, volumes[i], dollar_volumes[i], tick_rules[i])
                
                # Accumulate based on tick direction
                b_t = tick_rules[i]
                
                if self.bar_type == BarType.TICK_RUNS:
                    # Count ticks
                    if b_t > 0:
                        buy_accumulator += 1
                    else:
                        sell_accumulator += 1
                elif self.bar_type == BarType.VOLUME_RUNS:
                    # Accumulate volume
                    if b_t > 0:
                        buy_accumulator += volumes[i]
                    else:
                        sell_accumulator += volumes[i]
                elif self.bar_type == BarType.DOLLAR_RUNS:
                    # Accumulate dollar volume
                    if b_t > 0:
                        buy_accumulator += dollar_volumes[i]
                    else:
                        sell_accumulator += dollar_volumes[i]
                
                # θ_T = max{buy_accumulator, sell_accumulator}
                theta_t = max(buy_accumulator, sell_accumulator)
                
                # Calculate threshold
                if self.bar_type == BarType.TICK_RUNS:
                    # E_0[θ_T] = E_0[T] * max{P[b_t=1], P[b_t=-1]}
                    expected_threshold = self.ewma_state.T * max(
                        self.ewma_state.buy_probability,
                        1 - self.ewma_state.buy_probability
                    )
                else:
                    # E_0[θ_T] = E_0[T] * max{P[b_t=1]*E[v_t|b_t=1], P[b_t=-1]*E[v_t|b_t=-1]}
                    if self.ewma_state.warm_up_count < warm_up_bars:
                        expected_threshold = self.ewma_state.T * 0.5
                    else:
                        buy_expectation = self.ewma_state.buy_probability * self.ewma_state.buy_volume
                        sell_expectation = (1 - self.ewma_state.buy_probability) * self.ewma_state.sell_volume
                        expected_threshold = self.ewma_state.T * max(buy_expectation, sell_expectation)
                
                self.chunk_buffer.processed_rows += 1
                
                # Check termination condition
                if theta_t >= expected_threshold:
                    bar_length = self.chunk_buffer.processed_rows - bar_start_idx
                    self._finalize_bar(current_bar, expected_threshold, bar_length)
                    self.pending_bars.append(current_bar)
                    self.total_bars_generated += 1
                    
                    # Save batch if needed
                    if len(self.pending_bars) >= self.bars_per_batch:
                        self._save_bars_batch(self.pending_bars)
                        self.pending_bars = []
                        self._check_memory_usage()
                    
                    # Update EWMA expectations with appropriate alphas
                    if self.bar_type == BarType.TICK_RUNS:
                        self._update_ewma_after_run_bar(
                            bar_length,
                            bar_stats,
                            alpha_T,
                            alpha_prob
                        )
                    else:
                        self._update_ewma_after_run_bar(
                            bar_length,
                            bar_stats,
                            alpha_T,
                            alpha_volumes
                        )
                    
                    # Reset for next bar
                    current_bar = None
                    buy_accumulator = 0
                    sell_accumulator = 0
                    bar_stats = self.chunk_buffer._init_bar_stats()
            
            # Update buffer state
            self.chunk_buffer.current_bar = current_bar
            self.chunk_buffer.buy_accumulator = buy_accumulator
            self.chunk_buffer.sell_accumulator = sell_accumulator
            self.chunk_buffer.bar_start_idx = bar_start_idx
            self.chunk_buffer.current_bar_stats = bar_stats
            self._save_state()
            
            gc.collect()
        
        # Handle incomplete bar
        if current_bar is not None:
            bar_length = self.chunk_buffer.processed_rows - bar_start_idx
            
            if self.bar_type == BarType.TICK_RUNS:
                threshold = self.ewma_state.T * max(
                    self.ewma_state.buy_probability,
                    1 - self.ewma_state.buy_probability
                )
            else:
                buy_exp = self.ewma_state.buy