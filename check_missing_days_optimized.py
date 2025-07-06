"""
Script para verificar dias faltantes nos arquivos optimized.parquet
"""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
import json
from loguru import logger

def analyze_optimized_files(base_dir: Path, symbol: str = "BTCUSDT", data_type: str = "futures", futures_type: str = "um"):
    """Analisa os arquivos optimized para encontrar dias faltantes"""
    
    # Diretório dos arquivos optimized
    if data_type == "spot":
        optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "spot"
    else:
        optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / f"futures-{futures_type}"
    
    if not optimized_dir.exists():
        logger.error(f"Diretório não encontrado: {optimized_dir}")
        return
    
    # Listar todos os arquivos optimized
    optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    logger.info(f"Encontrados {len(optimized_files)} arquivos optimized")
    
    all_dates = set()
    date_ranges = []
    
    # Analisar cada arquivo
    for file_path in optimized_files:
        logger.info(f"\nAnalisando {file_path.name}...")
        
        # Ler o arquivo parquet
        table = pq.read_table(file_path, columns=['time'])
        df = table.to_pandas()
        
        # Converter timestamps para datas
        df['date'] = pd.to_datetime(df['time']).dt.date
        
        # Obter range de datas
        min_date = df['date'].min()
        max_date = df['date'].max()
        unique_dates = set(df['date'].unique())
        
        logger.info(f"  Período: {min_date} a {max_date}")
        logger.info(f"  Dias únicos: {len(unique_dates)}")
        
        all_dates.update(unique_dates)
        date_ranges.append((min_date, max_date))
        
        # Verificar dias faltantes no período
        current_date = min_date
        missing_in_file = []
        while current_date <= max_date:
            if current_date not in unique_dates:
                missing_in_file.append(current_date)
            current_date += timedelta(days=1)
        
        if missing_in_file:
            logger.warning(f"  Dias faltantes no arquivo: {len(missing_in_file)}")
            for date in missing_in_file[:5]:  # Mostrar apenas os primeiros 5
                logger.warning(f"    - {date}")
            if len(missing_in_file) > 5:
                logger.warning(f"    ... e mais {len(missing_in_file) - 5} dias")
    
    # Análise global
    logger.info("\n=== ANÁLISE GLOBAL ===")
    
    # Período total coberto
    global_min = min(dr[0] for dr in date_ranges)
    global_max = max(dr[1] for dr in date_ranges)
    logger.info(f"Período total: {global_min} a {global_max}")
    
    # Verificar todos os dias faltantes
    all_missing_days = []
    current_date = global_min
    while current_date <= global_max:
        if current_date not in all_dates:
            all_missing_days.append(current_date)
        current_date += timedelta(days=1)
    
    logger.info(f"Total de dias únicos nos arquivos: {len(all_dates)}")
    logger.info(f"Total de dias esperados: {(global_max - global_min).days + 1}")
    logger.info(f"Total de dias faltantes: {len(all_missing_days)}")
    
    if all_missing_days:
        # Salvar lista de dias faltantes
        missing_file = base_dir / f"missing_days_optimized_{symbol}_{data_type}.json"
        missing_data = {
            "summary": {
                "total_missing": len(all_missing_days),
                "period_start": str(global_min),
                "period_end": str(global_max),
                "total_days_covered": len(all_dates),
                "total_days_expected": (global_max - global_min).days + 1
            },
            "missing_dates": [str(d) for d in sorted(all_missing_days)]
        }
        
        with open(missing_file, 'w') as f:
            json.dump(missing_data, f, indent=2)
        
        logger.info(f"\nLista de dias faltantes salva em: {missing_file}")
        
        # Mostrar alguns exemplos
        logger.info("\nPrimeiros dias faltantes:")
        for date in all_missing_days[:10]:
            logger.info(f"  - {date}")
        if len(all_missing_days) > 10:
            logger.info(f"  ... e mais {len(all_missing_days) - 10} dias")
    
    return all_missing_days

if __name__ == "__main__":
    base_dir = Path("datasets")
    
    # Analisar futures
    logger.info("=== ANALISANDO FUTURES ===")
    missing_futures = analyze_optimized_files(base_dir, data_type="futures")
    
    # Analisar spot se existir
    spot_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "spot"
    if spot_dir.exists() and any(spot_dir.glob("*.parquet")):
        logger.info("\n\n=== ANALISANDO SPOT ===")
        missing_spot = analyze_optimized_files(base_dir, data_type="spot")