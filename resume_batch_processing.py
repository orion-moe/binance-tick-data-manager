#!/usr/bin/env python3
"""
Script para retomar o processamento de onde parou
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


def get_current_status(optimized_dir: Path, symbol: str):
    """Verifica o status atual dos arquivos optimized"""
    merger = ParquetMerger(symbol=symbol)
    last_optimized = merger.find_last_optimized_file(optimized_dir)
    
    if not last_optimized:
        return None, None
    
    # Obter última data
    pf = pq.ParquetFile(last_optimized)
    last_group = pf.read_row_group(pf.num_row_groups - 1, columns=['time'])
    df_last = last_group.to_pandas()
    
    last_timestamp = df_last['time'].max()
    last_date = pd.to_datetime(last_timestamp).date()
    
    # Obter tamanho do arquivo
    file_size_gb = last_optimized.stat().st_size / (1024**3)
    
    return last_optimized, last_date, file_size_gb


def find_remaining_files(daily_dir: Path, symbol: str, after_date: datetime) -> list:
    """Encontra arquivos diários ainda não processados"""
    remaining_files = []
    
    pattern = f"{symbol}-Trades-*.parquet"
    for file in sorted(daily_dir.glob(pattern)):
        try:
            date_str = file.stem.split('-Trades-')[-1]
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            
            if file_date > after_date:
                remaining_files.append(file)
        except:
            continue
    
    return remaining_files


def main():
    # Configurações
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    data_type = "futures"
    futures_type = "um"
    
    logger.info(f"=== RETOMANDO PROCESSAMENTO PARA {symbol} {data_type}/{futures_type} ===")
    
    # Diretórios
    optimized_dir = base_dir / f"dataset-raw-daily-compressed-optimized" / f"futures-{futures_type}"
    daily_dir = base_dir / f"dataset-raw-daily-compressed" / f"futures-{futures_type}"
    
    # Verificar status atual
    last_optimized, last_date, file_size_gb = get_current_status(optimized_dir, symbol)
    
    if not last_optimized:
        logger.error("Nenhum arquivo optimized encontrado!")
        return
    
    logger.info(f"Status atual:")
    logger.info(f"  Último arquivo: {last_optimized.name}")
    logger.info(f"  Tamanho: {file_size_gb:.2f} GB")
    logger.info(f"  Última data processada: {last_date}")
    
    # Encontrar arquivos restantes
    remaining_files = find_remaining_files(daily_dir, symbol, pd.to_datetime(last_date))
    
    if not remaining_files:
        logger.info("✅ Todos os arquivos já foram processados!")
        return
    
    logger.info(f"\nArquivos restantes: {len(remaining_files)}")
    logger.info(f"Próximo arquivo: {remaining_files[0].name}")
    logger.info(f"Último arquivo: {remaining_files[-1].name}")
    
    # Inicializar merger
    merger = ParquetMerger(symbol=symbol)
    
    # Processar em lotes
    batch_size = 20  # Lotes menores para evitar problemas
    total_processed = 0
    total_rows_added = 0
    
    logger.info(f"\nRetomando processamento em lotes de {batch_size} arquivos...")
    
    try:
        for i in range(0, len(remaining_files), batch_size):
            batch_files = remaining_files[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(remaining_files) + batch_size - 1) // batch_size
            
            logger.info(f"\n=== LOTE {batch_num}/{total_batches} ===")
            logger.info(f"Arquivos: {len(batch_files)}")
            logger.info(f"De: {batch_files[0].name}")
            logger.info(f"Até: {batch_files[-1].name}")
            
            # Processar lote
            files_merged, rows_added = merger.merge_daily_files(
                optimized_dir=optimized_dir,
                daily_dir=daily_dir,
                daily_files=batch_files,
                max_file_size_gb=10.0
            )
            
            total_processed += len(batch_files)
            total_rows_added += rows_added
            
            logger.success(f"✅ Lote {batch_num} concluído: {rows_added:,} linhas")
            
            # Verificar tamanho do arquivo atual
            current_optimized, _, current_size_gb = get_current_status(optimized_dir, symbol)
            logger.info(f"Tamanho atual do arquivo: {current_size_gb:.2f} GB")
            
            # Limpar memória
            gc.collect()
            
            # Pequena pausa entre lotes
            import time
            time.sleep(2)
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Processamento interrompido pelo usuário")
        logger.info(f"Arquivos processados até agora: {total_processed}")
        logger.info(f"Linhas adicionadas: {total_rows_added:,}")
        return
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        logger.info(f"Arquivos processados até o erro: {total_processed}")
        raise
    
    # Resultado final
    logger.success(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
    logger.info(f"Total de arquivos processados: {total_processed}")
    logger.info(f"Total de linhas adicionadas: {total_rows_added:,}")
    
    # Status final
    final_optimized, final_date, final_size_gb = get_current_status(optimized_dir, symbol)
    logger.info(f"\nStatus final:")
    logger.info(f"  Última data: {final_date}")
    logger.info(f"  Tamanho final: {final_size_gb:.2f} GB")
    
    # Listar todos os arquivos optimized
    all_optimized = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    logger.info(f"\nTotal de arquivos optimized: {len(all_optimized)}")
    for f in all_optimized[-3:]:  # Mostrar últimos 3
        size = f.stat().st_size / (1024**3)
        logger.info(f"  - {f.name}: {size:.2f} GB")


if __name__ == "__main__":
    main()