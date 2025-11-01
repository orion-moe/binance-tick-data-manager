#!/usr/bin/env python3
"""
Script to count lines in compressed parquet files from daily raw spot and futures-um data.
"""

import os
import glob
import pandas as pd

def count_parquet_lines(file_path):
    """Count number of rows in a parquet file."""
    try:
        df = pd.read_parquet(file_path)
        return len(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0

def main():
    # Base directories for data - trying both compressed and optimized
    base_dirs = [
        "data/dataset-raw-daily-compressed",
        "data/dataset-raw-daily-compressed-optimized"
    ]
    
    # Directories to scan
    directories = ["spot", "futures-um"]
    
    total_lines = 0
    total_files = 0
    
    print("Contando linhas em arquivos parquet:")
    print("=" * 60)
    
    for base_dir in base_dirs:
        print(f"\nDiretório base: {base_dir}")
        print("=" * 60)
        
        for directory in directories:
            dir_path = os.path.join(base_dir, directory)
            
            if not os.path.exists(dir_path):
                print(f"Diretório não encontrado: {dir_path}")
                continue
                
            # Find all parquet files
            pattern = os.path.join(dir_path, "*.parquet")
            files = glob.glob(pattern)
            
            print(f"\n{directory.upper()}:")
            print("-" * 40)
            
            if not files:
                print("  Nenhum arquivo .parquet encontrado")
                continue
                
            dir_lines = 0
            for file_path in sorted(files):
                filename = os.path.basename(file_path)
                lines = count_parquet_lines(file_path)
                print(f"  {filename}: {lines:,} linhas")
                dir_lines += lines
                total_files += 1
                
            print(f"  Total {directory}: {dir_lines:,} linhas")
            total_lines += dir_lines
    
    print("\n" + "=" * 60)
    print(f"TOTAL GERAL: {total_lines:,} linhas em {total_files} arquivos")

if __name__ == "__main__":
    main()