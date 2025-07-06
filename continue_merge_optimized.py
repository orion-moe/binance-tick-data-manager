#!/usr/bin/env python3
"""
Script otimizado para continuar o merge com melhor gestão de memória
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys
import gc
import shutil

# Configurar logging
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def get_last_date_optimized(file_path: Path):
    """Obtém a última data de um arquivo optimized de forma eficiente"""
    pf = pq.ParquetFile(file_path)
    last_group = pf.read_row_group(pf.num_row_groups - 1, columns=['time'])
    df_last = last_group.to_pandas()
    last_timestamp = df_last['time'].max()
    return pd.to_datetime(last_timestamp).date()


def create_new_optimized_file(data: pd.DataFrame, optimized_dir: Path, symbol: str, file_number: int):
    """Cria um novo arquivo optimized"""
    output_path = optimized_dir / f"{symbol}-Trades-Optimized-{file_number:03d}.parquet"
    
    logger.info(f"Criando novo arquivo: {output_path.name}")
    table = pa.Table.from_pandas(data)
    pq.write_table(table, output_path, compression='snappy')
    
    size_gb = output_path.stat().st_size / (1024**3)
    logger.success(f"✅ Criado {output_path.name} ({size_gb:.2f} GB) com {len(data):,} linhas")
    
    return output_path


def process_remaining_files():
    """Processa os arquivos restantes criando novos arquivos optimized"""
    
    # Configurações
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "futures-um"
    daily_dir = base_dir / "dataset-raw-daily-compressed" / "futures-um"
    
    # Verificar último arquivo optimized
    optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    if not optimized_files:
        logger.error("Nenhum arquivo optimized encontrado!")
        return
    
    last_optimized = optimized_files[-1]
    last_number = int(last_optimized.stem.split('-')[-1])
    
    # Obter última data processada
    last_date = get_last_date_optimized(last_optimized)
    logger.info(f"Última data processada: {last_date}")
    
    # Encontrar arquivos não processados
    daily_files = []
    pattern = f"{symbol}-Trades-*.parquet"
    
    for file in sorted(daily_dir.glob(pattern)):
        try:
            date_str = file.stem.split('-Trades-')[-1]
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            if file_date > last_date:
                daily_files.append(file)
        except:
            continue
    
    if not daily_files:
        logger.info("✅ Todos os arquivos já foram processados!")
        return
    
    logger.info(f"Arquivos a processar: {len(daily_files)}")
    logger.info(f"Período: {daily_files[0].name} até {daily_files[-1].name}")
    
    # Processar em grupos para criar novos arquivos optimized
    batch_size = 100  # ~100 dias por arquivo optimized
    current_file_number = last_number + 1
    total_processed = 0
    
    for i in range(0, len(daily_files), batch_size):
        batch_files = daily_files[i:i + batch_size]
        
        logger.info(f"\n=== Processando grupo {(i//batch_size) + 1} ===")
        logger.info(f"Arquivos: {len(batch_files)}")
        logger.info(f"De: {batch_files[0].name}")
        logger.info(f"Até: {batch_files[-1].name}")
        
        try:
            # Ler arquivos do lote
            dfs = []
            total_rows = 0
            
            for j, file in enumerate(batch_files):
                if j % 10 == 0:
                    logger.info(f"  Lendo arquivo {j+1}/{len(batch_files)}...")
                
                df = pd.read_parquet(file)
                dfs.append(df)
                total_rows += len(df)
                
                # Liberar memória periodicamente
                if j % 20 == 0:
                    gc.collect()
            
            logger.info(f"  Total de linhas lidas: {total_rows:,}")
            
            # Combinar e ordenar
            logger.info("  Combinando dados...")
            combined_df = pd.concat(dfs, ignore_index=True)
            del dfs  # Liberar memória
            gc.collect()
            
            logger.info("  Ordenando por tempo...")
            combined_df = combined_df.sort_values('time').reset_index(drop=True)
            
            # Criar novo arquivo optimized
            create_new_optimized_file(
                combined_df, 
                optimized_dir, 
                symbol, 
                current_file_number
            )
            
            current_file_number += 1
            total_processed += len(batch_files)
            
            # Liberar memória
            del combined_df
            gc.collect()
            
            logger.info(f"  Progresso: {total_processed}/{len(daily_files)} arquivos processados")
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar grupo: {e}")
            logger.info(f"Processados até agora: {total_processed} arquivos")
            raise
    
    logger.success(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
    logger.info(f"Total de arquivos diários processados: {total_processed}")
    logger.info(f"Novos arquivos optimized criados: {current_file_number - last_number - 1}")
    
    # Listar todos os arquivos optimized
    final_optimized = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    logger.info(f"\nTotal de arquivos optimized: {len(final_optimized)}")
    
    total_size = 0
    for f in final_optimized:
        size = f.stat().st_size / (1024**3)
        total_size += size
        if f.name.endswith(f"{last_number:03d}.parquet") or int(f.stem.split('-')[-1]) > last_number:
            logger.info(f"  - {f.name}: {size:.2f} GB")
    
    logger.info(f"\nTamanho total: {total_size:.2f} GB")


if __name__ == "__main__":
    logger.info("=== CONTINUANDO PROCESSAMENTO DOS ARQUIVOS OPTIMIZED ===")
    process_remaining_files()