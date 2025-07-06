#!/usr/bin/env python3
"""
Script otimizado para adicionar dias faltantes aos arquivos optimized em lotes
Processa arquivos em grupos menores para evitar problemas de memória
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys
import gc
from src.data_pipeline.processors.parquet_merger import ParquetMerger

# Configurar logging
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def find_daily_files_after_date(daily_dir: Path, symbol: str, after_date: datetime) -> list:
    """Encontra arquivos diários após uma data específica"""
    daily_files = []
    
    # Procurar por arquivos parquet diários
    pattern = f"{symbol}-Trades-*.parquet"
    for file in sorted(daily_dir.glob(pattern)):
        # Extrair data do nome do arquivo
        try:
            date_str = file.stem.split('-Trades-')[-1]
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            
            if file_date > after_date:
                daily_files.append(file)
        except:
            continue
    
    return daily_files


def process_in_batches(merger: ParquetMerger, optimized_dir: Path, daily_dir: Path, 
                      daily_files: list, batch_size: int = 30):
    """Processa arquivos em lotes para evitar problemas de memória"""
    
    total_files = len(daily_files)
    total_rows_added = 0
    files_processed = 0
    
    # Processar em lotes
    for i in range(0, total_files, batch_size):
        batch_files = daily_files[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        logger.info(f"\n=== PROCESSANDO LOTE {batch_num}/{total_batches} ===")
        logger.info(f"Arquivos neste lote: {len(batch_files)}")
        logger.info(f"De: {batch_files[0].name}")
        logger.info(f"Até: {batch_files[-1].name}")
        
        try:
            # Processar lote
            files_merged, rows_added = merger.merge_daily_files(
                optimized_dir=optimized_dir,
                daily_dir=daily_dir,
                daily_files=batch_files,
                max_file_size_gb=10.0
            )
            
            total_rows_added += rows_added
            files_processed += len(batch_files)
            
            logger.success(f"✅ Lote {batch_num} concluído: {rows_added:,} linhas adicionadas")
            
            # Limpar memória
            gc.collect()
            
        except Exception as e:
            logger.error(f"❌ Erro no lote {batch_num}: {e}")
            logger.info(f"Arquivos processados até agora: {files_processed}")
            logger.info(f"Linhas adicionadas até agora: {total_rows_added:,}")
            raise
    
    return files_processed, total_rows_added


def main():
    # Configurações
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    
    # Para spot
    data_type = "spot"
    
    logger.info(f"=== ADICIONANDO DIAS FALTANTES PARA {symbol} {data_type} (MODO BATCH) ===")
    
    # Diretórios
    optimized_dir = base_dir / f"dataset-raw-daily-compressed-optimized" / data_type
    daily_dir = base_dir / f"dataset-raw-daily-compressed" / data_type
    
    if not optimized_dir.exists():
        logger.error(f"Diretório optimized não encontrado: {optimized_dir}")
        return
    
    if not daily_dir.exists():
        logger.error(f"Diretório daily não encontrado: {daily_dir}")
        return
    
    # Inicializar merger
    merger = ParquetMerger(symbol=symbol)
    
    # Encontrar último arquivo optimized
    last_optimized = merger.find_last_optimized_file(optimized_dir)
    if not last_optimized:
        logger.error("Nenhum arquivo optimized encontrado!")
        return
    
    logger.info(f"Último arquivo optimized: {last_optimized.name}")
    
    # Obter última data do arquivo optimized
    logger.info("Analisando último arquivo optimized...")
    pf = pq.ParquetFile(last_optimized)
    
    # Ler apenas o último row group para obter a última data
    last_group = pf.read_row_group(pf.num_row_groups - 1, columns=['time'])
    df_last = last_group.to_pandas()
    
    last_timestamp = df_last['time'].max()
    last_date = pd.to_datetime(last_timestamp).date()
    logger.info(f"Última data no arquivo optimized: {last_date}")
    
    # Encontrar arquivos diários após essa data
    logger.info(f"\nProcurando arquivos diários após {last_date}...")
    daily_files = find_daily_files_after_date(daily_dir, symbol, pd.to_datetime(last_date))
    
    if not daily_files:
        logger.info("Nenhum arquivo diário novo encontrado!")
        return
    
    logger.info(f"Encontrados {len(daily_files)} arquivos diários para adicionar")
    logger.info(f"Período: {daily_files[0].name} até {daily_files[-1].name}")
    
    # Processar em lotes
    batch_size = 30  # Processar 30 arquivos por vez (aproximadamente 1 mês)
    logger.info(f"\nProcessando em lotes de {batch_size} arquivos...")
    
    try:
        files_processed, total_rows_added = process_in_batches(
            merger, optimized_dir, daily_dir, daily_files, batch_size
        )
        
        logger.success(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
        logger.info(f"Total de arquivos processados: {files_processed}")
        logger.info(f"Total de linhas adicionadas: {total_rows_added:,}")
        
        # Verificar o resultado
        logger.info("\nVerificando resultado final...")
        last_optimized_after = merger.find_last_optimized_file(optimized_dir)
        
        # Obter nova última data
        pf_after = pq.ParquetFile(last_optimized_after)
        last_group_after = pf_after.read_row_group(pf_after.num_row_groups - 1, columns=['time'])
        df_last_after = last_group_after.to_pandas()
        
        new_last_timestamp = df_last_after['time'].max()
        new_last_date = pd.to_datetime(new_last_timestamp).date()
        
        logger.info(f"Nova última data: {new_last_date}")
        logger.info(f"Período adicionado: {last_date} até {new_last_date}")
        logger.info(f"Total de dias adicionados: {(new_last_date - last_date).days}")
        
        # Informar sobre arquivos optimized
        optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
        logger.info(f"\nTotal de arquivos optimized: {len(optimized_files)}")
        for f in optimized_files:
            size_gb = f.stat().st_size / (1024**3)
            logger.info(f"  - {f.name}: {size_gb:.2f} GB")
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        raise


if __name__ == "__main__":
    main()