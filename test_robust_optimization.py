#!/usr/bin/env python3
"""
Test script para demonstrar o novo sistema robusto de otimização
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_pipeline.processors.robust_parquet_optimizer import RobustParquetOptimizer, OptimizationConfig
from data_pipeline.validators.data_integrity_validator import DataIntegrityValidator


def test_robust_optimization():
    """Teste do otimizador robusto"""
    print("="*60)
    print("TESTE DO OTIMIZADOR ROBUSTO DE PARQUET")
    print("="*60)
    
    # Configuração para teste
    source_dir = "datasets/dataset-raw-monthly-compressed/spot"
    target_dir = "datasets/dataset-raw-monthly-compressed-optimized-test/spot"
    
    # Configuração robusta
    config = OptimizationConfig(
        max_file_size_gb=5,  # Arquivos menores para teste
        compression='snappy',
        verify_checksum=True,
        keep_backup=True,
        max_workers=1  # Processamento conservativo
    )
    
    print(f"📁 Source: {source_dir}")
    print(f"📁 Target: {target_dir}")
    print(f"⚙️ Config: {config}")
    
    # Criar otimizador
    optimizer = RobustParquetOptimizer(source_dir, target_dir, config)
    
    # Executar otimização
    print("\n🚀 Iniciando otimização robusta...")
    success = optimizer.run_optimization()
    
    if success:
        print("\n✅ Otimização concluída com sucesso!")
        
        # Validação pós-otimização
        print("\n🔍 Executando validação de integridade...")
        validator = DataIntegrityValidator()
        report = validator.validate_directory(Path(target_dir))
        
        validator.print_report_summary(report)
        
        if report.invalid_files == 0:
            print("\n🎉 Todos os arquivos otimizados passaram na validação!")
        else:
            print(f"\n⚠️ {report.invalid_files} arquivos falharam na validação")
            
    else:
        print("\n❌ Otimização falhou")
        
    return success


def test_integrity_validation():
    """Teste do validador de integridade"""
    print("\n" + "="*60)
    print("TESTE DO VALIDADOR DE INTEGRIDADE")
    print("="*60)
    
    # Testar diretório de arquivos originais
    test_dir = "datasets/dataset-raw-monthly-compressed/spot"
    
    if not Path(test_dir).exists():
        print(f"❌ Diretório de teste não encontrado: {test_dir}")
        return False
    
    print(f"📁 Testando diretório: {test_dir}")
    
    # Criar validador
    validator = DataIntegrityValidator()
    
    # Executar validação
    print("\n🔍 Executando validação completa...")
    report = validator.validate_directory(Path(test_dir), max_workers=2)
    
    # Exibir resultados
    validator.print_report_summary(report)
    
    # Salvar relatório
    report_path = Path("reports/test_validation_report.json")
    report_path.parent.mkdir(exist_ok=True)
    validator.save_report(report, report_path)
    print(f"\n📄 Relatório salvo em: {report_path}")
    
    return report.invalid_files == 0


def main():
    """Função principal de teste"""
    print("🧪 INICIANDO TESTES DO SISTEMA ROBUSTO")
    print("="*60)
    
    # Teste 1: Validação de integridade
    print("\n📋 Teste 1: Validação de Integridade")
    validation_success = test_integrity_validation()
    
    # Teste 2: Otimização robusta (apenas se validação passou)
    if validation_success:
        print("\n📋 Teste 2: Otimização Robusta")
        optimization_success = test_robust_optimization()
    else:
        print("\n⚠️ Pulando teste de otimização devido à falha na validação")
        optimization_success = False
    
    # Resultados finais
    print("\n" + "="*60)
    print("RESULTADOS DOS TESTES")
    print("="*60)
    print(f"Validação de Integridade: {'✅ PASSOU' if validation_success else '❌ FALHOU'}")
    print(f"Otimização Robusta: {'✅ PASSOU' if optimization_success else '❌ FALHOU'}")
    
    if validation_success and optimization_success:
        print("\n🎉 TODOS OS TESTES PASSARAM!")
        print("🛡️ O sistema robusto está funcionando corretamente")
    else:
        print("\n⚠️ ALGUNS TESTES FALHARAM")
        print("💡 Verifique os logs para mais detalhes")


if __name__ == "__main__":
    main()