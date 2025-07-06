"""
Script para analisar dias faltantes e adicionar aos arquivos optimized
"""
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from datetime import datetime, timedelta
import json
from loguru import logger
import sys

# Configurar logging
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")

def quick_analyze_parquet(file_path: Path):
    """Análise rápida de um arquivo parquet - apenas primeira e última data"""
    # Ler apenas metadados
    parquet_file = pq.ParquetFile(file_path)
    
    # Obter número de rows
    num_rows = parquet_file.metadata.num_rows
    
    # Ler apenas primeira e última row para obter range de datas
    first_batch = parquet_file.read_row_group(0, columns=['time']).to_pandas()
    last_group_idx = parquet_file.num_row_groups - 1
    last_batch = parquet_file.read_row_group(last_group_idx, columns=['time']).to_pandas()
    
    # Converter para datas
    first_date = pd.to_datetime(first_batch['time'].iloc[0]).date()
    last_date = pd.to_datetime(last_batch['time'].iloc[-1]).date()
    
    return first_date, last_date, num_rows

def find_missing_days_in_optimized(base_dir: Path, symbol: str = "BTCUSDT", data_type: str = "futures", futures_type: str = "um"):
    """Encontra dias faltantes nos arquivos optimized de forma eficiente"""
    
    # Diretório dos arquivos optimized
    if data_type == "spot":
        optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "spot"
    else:
        optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / f"futures-{futures_type}"
    
    if not optimized_dir.exists():
        logger.error(f"Diretório não encontrado: {optimized_dir}")
        return []
    
    # Listar todos os arquivos optimized
    optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    logger.info(f"Encontrados {len(optimized_files)} arquivos optimized")
    
    # Analisar ranges de cada arquivo
    file_ranges = []
    for file_path in optimized_files:
        logger.info(f"Analisando {file_path.name}...")
        first_date, last_date, num_rows = quick_analyze_parquet(file_path)
        file_ranges.append({
            'file': file_path.name,
            'first_date': first_date,
            'last_date': last_date,
            'num_rows': num_rows
        })
        logger.info(f"  Período: {first_date} a {last_date} ({num_rows:,} rows)")
    
    # Encontrar período total
    global_min = min(fr['first_date'] for fr in file_ranges)
    global_max = max(fr['last_date'] for fr in file_ranges)
    
    logger.info(f"\nPeríodo total coberto: {global_min} a {global_max}")
    
    # Para encontrar dias faltantes, precisamos verificar cada arquivo
    # Vamos fazer uma verificação mais detalhada apenas nos períodos suspeitos
    logger.info("\nVerificando continuidade entre arquivos...")
    
    # Ordenar por data inicial
    file_ranges.sort(key=lambda x: x['first_date'])
    
    gaps = []
    for i in range(len(file_ranges) - 1):
        current = file_ranges[i]
        next_file = file_ranges[i + 1]
        
        # Verificar se há gap entre arquivos
        expected_next = current['last_date'] + timedelta(days=1)
        if expected_next < next_file['first_date']:
            gap_start = expected_next
            gap_end = next_file['first_date'] - timedelta(days=1)
            gap_days = (gap_end - gap_start).days + 1
            gaps.append({
                'start': gap_start,
                'end': gap_end,
                'days': gap_days,
                'between_files': (current['file'], next_file['file'])
            })
            logger.warning(f"  Gap encontrado: {gap_start} a {gap_end} ({gap_days} dias)")
    
    return gaps, file_ranges, global_min, global_max

def get_available_daily_files(base_dir: Path, symbol: str, data_type: str, futures_type: str, missing_dates: list):
    """Verifica quais arquivos diários estão disponíveis para os dias faltantes"""
    
    # Diretório dos arquivos diários comprimidos
    if data_type == "spot":
        daily_dir = base_dir / "dataset-raw-daily-compressed" / "spot"
    else:
        daily_dir = base_dir / "dataset-raw-daily-compressed" / f"futures-{futures_type}"
    
    available_files = []
    missing_files = []
    
    for date in missing_dates:
        date_str = date.strftime('%Y-%m-%d')
        parquet_file = daily_dir / f"{symbol}-Trades-{date_str}.parquet"
        
        if parquet_file.exists():
            available_files.append((date, parquet_file))
        else:
            missing_files.append(date)
    
    return available_files, missing_files

def main():
    base_dir = Path("datasets")
    symbol = "BTCUSDT"
    data_type = "futures"
    futures_type = "um"
    
    # 1. Encontrar gaps nos arquivos optimized
    logger.info("=== ANALISANDO ARQUIVOS OPTIMIZED ===")
    gaps, file_ranges, global_min, global_max = find_missing_days_in_optimized(
        base_dir, symbol, data_type, futures_type
    )
    
    if not gaps:
        logger.info("Nenhum gap encontrado entre os arquivos!")
        return
    
    # 2. Coletar todas as datas faltantes
    all_missing_dates = []
    for gap in gaps:
        current = gap['start']
        while current <= gap['end']:
            all_missing_dates.append(current)
            current += timedelta(days=1)
    
    logger.info(f"\nTotal de dias faltantes: {len(all_missing_dates)}")
    
    # 3. Verificar quais arquivos diários estão disponíveis
    logger.info("\n=== VERIFICANDO ARQUIVOS DIÁRIOS DISPONÍVEIS ===")
    available_files, missing_files = get_available_daily_files(
        base_dir, symbol, data_type, futures_type, all_missing_dates
    )
    
    logger.info(f"Arquivos disponíveis para recuperação: {len(available_files)}")
    logger.info(f"Arquivos que precisam ser baixados/extraídos: {len(missing_files)}")
    
    # 4. Salvar relatório
    report = {
        "analysis_date": datetime.now().isoformat(),
        "symbol": symbol,
        "data_type": data_type,
        "futures_type": futures_type,
        "period": {
            "start": str(global_min),
            "end": str(global_max)
        },
        "gaps": [
            {
                "start": str(gap['start']),
                "end": str(gap['end']),
                "days": gap['days'],
                "between_files": gap['between_files']
            }
            for gap in gaps
        ],
        "missing_dates": {
            "total": len(all_missing_dates),
            "available_for_recovery": len(available_files),
            "need_download": len(missing_files),
            "dates_need_download": [str(d) for d in missing_files[:20]]  # Primeiros 20
        }
    }
    
    report_file = base_dir / f"missing_days_report_{symbol}_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"\nRelatório salvo em: {report_file}")
    
    # 5. Mostrar próximos passos
    if missing_files:
        logger.info("\n=== PRÓXIMOS PASSOS ===")
        logger.info("1. Baixar/extrair os arquivos faltantes:")
        for date in missing_files[:10]:
            logger.info(f"   - {date}")
        if len(missing_files) > 10:
            logger.info(f"   ... e mais {len(missing_files) - 10} arquivos")
        
        logger.info("\n2. Após baixar/extrair, execute o script de adição aos optimized")
    
    return available_files, missing_files, gaps

if __name__ == "__main__":
    available_files, missing_files, gaps = main()