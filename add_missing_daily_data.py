#!/usr/bin/env python3
"""
Script para atualizar automaticamente os dados diários faltantes para
os mercados BTCUSDT Spot e Futures da Binance.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import pyarrow.parquet as pq
import concurrent.futures

# Adicionar o diretório 'src' ao path do Python para encontrar os módulos
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Importar os componentes necessários do pipeline
from src.data_pipeline.extractors.csv_extractor import CSVExtractor
from src.data_pipeline.converters.csv_to_parquet import CSVToParquetConverter
from src.data_pipeline.downloaders.binance_downloader import BinanceDataDownloader
from src.data_pipeline.processors.parquet_merger import ParquetMerger


def setup_logging():
    """Configura o sistema de logs."""
    log_dir = Path("datasets/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_filename = f"update_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = log_dir / log_filename

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.info(f"Script de atualização iniciado - Log: {log_path}")
    return log_path


class PipelineConfig:
    """Armazena a configuração do pipeline para um mercado específico."""
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.data_type = None
        self.futures_type = None
        self.granularity = "daily"


def _execute_update_for_market(config: PipelineConfig):
    """Função principal que executa a atualização para um mercado (Spot ou Futures)."""
    logging.info(f"Analisando status dos dados para {config.symbol} {config.data_type.upper()}...")

    # Define o diretório dos arquivos otimizados
    if config.data_type == "spot":
        optimized_dir = Path("datasets") / "dataset-raw-daily-compressed-optimized" / "spot"
    else:
        optimized_dir = Path("datasets") / "dataset-raw-daily-compressed-optimized" / f"futures-{config.futures_type}"

    last_timestamp = None
    optimized_dir.mkdir(parents=True, exist_ok=True)
    parquet_files = sorted(optimized_dir.glob(f"{config.symbol}-Trades-*.parquet"))

    if parquet_files:
        last_file = parquet_files[-1]
        logging.info(f"Verificando último arquivo: {last_file.name}")
        try:
            df = pd.read_parquet(last_file, columns=['time'])
            if not df.empty:
                last_timestamp = pd.to_datetime(df['time'].max())
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo parquet {last_file.name}: {e}")

    if not last_timestamp:
        logging.warning(f"Nenhum dado encontrado ou arquivo corrompido para {config.data_type}. Usando data inicial padrão.")
        if config.data_type == "spot":
            last_timestamp = datetime.strptime("2017-08-16", "%Y-%m-%d")
        else: # futures
            last_timestamp = datetime.strptime("2019-09-07", "%Y-%m-%d")

    logging.info(f"Último ponto de dado encontrado: {last_timestamp}")

    # Calcula o intervalo de datas para baixar
    current_date = datetime.now()
    start_date = last_timestamp.date() + timedelta(days=1)
    end_date = current_date.date() - timedelta(days=1)

    if start_date > end_date:
        logging.info(f"Os dados para {config.data_type.upper()} já estão atualizados. Nenhuma ação necessária.")
        return

    logging.info(f"Intervalo de atualização para {config.data_type.upper()}: de {start_date} a {end_date}")

    # Gera a lista de datas
    dates_to_download = []
    current_dt = start_date
    while current_dt <= end_date:
        dates_to_download.append(datetime.combine(current_dt, datetime.min.time()))
        current_dt += timedelta(days=1)

    # --- Início do Mini-Pipeline de Atualização ---
    try:
        # 1. Download
        downloader = BinanceDataDownloader(
            symbol=config.symbol, data_type=config.data_type, futures_type=config.futures_type,
            granularity="daily", base_dir=Path("datasets")
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(downloader.download_with_checksum, dates_to_download))

        # 2. Extração
        extractor = CSVExtractor(
            symbol=config.symbol, data_type=config.data_type, futures_type=config.futures_type,
            granularity="daily"
        )
        extractor.extract_and_verify_all()

        # 3. Conversão para Parquet
        converter = CSVToParquetConverter(
            symbol=config.symbol, data_type=config.data_type, futures_type=config.futures_type,
            granularity="daily"
        )
        converter.convert_all_csv_files()

        # 4. Merge
        if config.data_type == "spot":
            daily_parquet_dir = Path("datasets") / "dataset-raw-daily-compressed" / "spot"
        else:
            daily_parquet_dir = Path("datasets") / "dataset-raw-daily-compressed" / f"futures-{config.futures_type}"

        new_daily_files = [f for f in daily_parquet_dir.glob("*.parquet") if any(d.strftime('%Y-%m-%d') in f.name for d in dates_to_download)]

        if new_daily_files:
            merger = ParquetMerger(symbol=config.symbol)
            merger.merge_daily_files(
                optimized_dir=optimized_dir, daily_dir=daily_parquet_dir, daily_files=new_daily_files,
                max_file_size_gb=10.0, delete_after_merge=True
            )
            logging.info(f"Merge de {len(new_daily_files)} arquivos para {config.data_type.upper()} concluído.")
        else:
            logging.warning("Nenhum novo arquivo parquet encontrado para o merge.")

    except Exception as e:
        logging.error(f"O pipeline de atualização falhou para {config.data_type.upper()}: {e}", exc_info=True)


def main():
    """
    Ponto de entrada principal do script. Executa a atualização automática.
    """
    log_path = setup_logging()

    print("\n" + "="*60)
    print(" 🚀 Bitcoin ML Finance - Atualização Automática de Dados ")
    print("="*60)
    print(f"📝 Logs detalhados estão sendo salvos em: {log_path}")
    print("\nEste script irá verificar e baixar automaticamente os dados diários")
    print("faltantes para BTCUSDT Spot e Futures (USD-M).")

    # --- Processar BTC Spot ---
    print("\n" + "#"*60)
    print(" ### ETAPA 1: Processando BTCUSDT Spot ### ")
    print("#"*60)
    spot_config = PipelineConfig()
    spot_config.data_type = "spot"
    spot_config.futures_type = "um"
    _execute_update_for_market(spot_config)

    # --- Processar BTC Futures ---
    print("\n" + "#"*60)
    print(" ### ETAPA 2: Processando BTCUSDT Futures (USD-M) ### ")
    print("#"*60)
    futures_config = PipelineConfig()
    futures_config.data_type = "futures"
    futures_config.futures_type = "um"
    _execute_update_for_market(futures_config)

    print("\n" + "="*60)
    print(" ✅ Processo de atualização concluído para todos os mercados. ")
    print("="*60)


if __name__ == "__main__":
    main()