#!/usr/bin/env python3
"""
Script para adicionar dias faltantes aos arquivos optimized
Específico para corrigir o problema com futures/um
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys
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


def main():
    # Configurações
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    
    # Para futures/um
    data_type = "futures"
    futures_type = "um"
    
    logger.info(f"=== ADICIONANDO DIAS FALTANTES PARA {symbol} {data_type}/{futures_type} ===")
    
    # Diretórios
    optimized_dir = base_dir / f"dataset-raw-daily-compressed-optimized" / f"futures-{futures_type}"
    daily_dir = base_dir / f"dataset-raw-daily-compressed" / f"futures-{futures_type}"
    
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
    
    logger.info(f"Encontrados {len(daily_files)} arquivos diários para adicionar:")
    for f in daily_files[:10]:  # Mostrar primeiros 10
        logger.info(f"  - {f.name}")
    if len(daily_files) > 10:
        logger.info(f"  ... e mais {len(daily_files) - 10} arquivos")
    
    # Confirmar antes de proceder
    logger.info(f"\nPronto para adicionar {len(daily_files)} arquivos ao optimized.")
    
    # Executar o merge
    try:
        logger.info("\nIniciando merge...")
        files_merged, rows_added = merger.merge_daily_files(
            optimized_dir=optimized_dir,
            daily_dir=daily_dir,
            daily_files=daily_files,
            max_file_size_gb=10.0
        )
        
        logger.success(f"\n✅ Merge concluído!")
        logger.info(f"Arquivos processados: {files_merged}")
        logger.info(f"Linhas adicionadas: {rows_added:,}")
        
        # Verificar o resultado
        logger.info("\nVerificando resultado...")
        last_optimized_after = merger.find_last_optimized_file(optimized_dir)
        
        # Obter nova última data
        pf_after = pq.ParquetFile(last_optimized_after)
        last_group_after = pf_after.read_row_group(pf_after.num_row_groups - 1, columns=['time'])
        df_last_after = last_group_after.to_pandas()
        
        new_last_timestamp = df_last_after['time'].max()
        new_last_date = pd.to_datetime(new_last_timestamp).date()
        
        logger.info(f"Nova última data: {new_last_date}")
        logger.info(f"Dias adicionados: {(new_last_date - last_date).days}")
        
        # Perguntar sobre limpeza
        logger.info("\n⚠️  Os arquivos diários foram mantidos. Para removê-los, execute:")
        logger.info(f"   merger.cleanup_merged_daily_files(daily_files, dry_run=False)")
        
    except Exception as e:
        logger.error(f"Erro durante o merge: {e}")
        raise


if __name__ == "__main__":
    main()