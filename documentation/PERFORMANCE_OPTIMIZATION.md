# 🚀 Otimização de Performance - Standard Dollar Bars

## ✅ Otimizações Implementadas

### 1. **Detecção Automática de CPU**
O sistema agora detecta automaticamente o número de CPUs disponíveis e configura o Dask para usar 100% da capacidade.

### 2. **Configuração Inteligente do Dask**
```python
# Antes (fixo)
n_workers=10, threads_per_worker=1  # Apenas 10 threads

# Agora (automático)
n_workers = cpu_count // 2
threads_per_worker = cpu_count // n_workers
# Ex: 16 CPUs = 8 workers × 2 threads = 16 threads totais
```

### 3. **Gestão Automática de Memória**
- Usa 90% da memória disponível
- Divide automaticamente entre workers
- Previne overflow de memória

## 📊 Monitoramento

### Para verificar uso de CPU:
```bash
# No terminal 1 - Monitorar CPU
htop

# No terminal 2 - Rodar o pipeline
python main.py
```

### Indicadores de boa performance:
- ✅ CPU próximo de 100%
- ✅ Todos os cores sendo utilizados
- ✅ Memória estável (não subindo constantemente)

## ⚙️ Ajuste Fino (Opcional)

Se quiser ajustar manualmente, edite `main.py`:

```python
# Para máxima performance (use com cuidado!)
client = setup_dask_client(
    n_workers=cpu_count,           # 1 worker por CPU
    threads_per_worker=1,           # 1 thread por worker
    memory_limit='8GB'              # Ajuste conforme sua RAM
)

# Para balanceamento (recomendado)
client = setup_dask_client(
    n_workers=cpu_count // 2,      # Metade dos CPUs
    threads_per_worker=2,           # 2 threads por worker
    memory_limit=None               # Auto-detect
)

# Para economia de memória
client = setup_dask_client(
    n_workers=4,                   # Poucos workers
    threads_per_worker=4,           # Mais threads
    memory_limit='4GB'              # Limite baixo
)
```

## 🔥 Dicas Extras para Máxima Performance

### 1. **Feche outros programas**
```bash
# Veja processos consumindo CPU
ps aux | sort -rn -k 3 | head -10
```

### 2. **Desative throttling térmico (notebooks)**
```bash
# MacOS - Performance máxima
sudo pmset -a gpuswitch 0
sudo pmset -a powermode 2

# Linux
sudo cpupower frequency-set -g performance
```

### 3. **Use SSD para dados**
- Mova os dados para SSD se estiverem em HD
- Garanta espaço livre no disco (>20%)

### 4. **Monitore a temperatura**
```bash
# MacOS
sudo powermetrics --samplers smc | grep -i temp

# Linux
sensors
```

## 📈 Performance Esperada

### Antes da otimização:
- CPU: 30-50% uso
- Tempo: X minutos

### Depois da otimização:
- CPU: 90-100% uso
- Tempo: ~X/2 minutos (até 2x mais rápido)

## 🆘 Troubleshooting

### Se CPU ainda não está em 100%:
1. **Verifique I/O do disco**
   ```bash
   iostat -x 1  # Linux
   iostat 1      # MacOS
   ```
   Se o disco está em 100%, o gargalo é I/O, não CPU.

2. **Aumente workers**
   ```python
   client = setup_dask_client(n_workers=cpu_count)
   ```

3. **Verifique se há swap**
   ```bash
   free -h       # Linux
   vm_stat       # MacOS
   ```
   Se está usando swap, reduza memory_limit.

### Se o processo trava:
1. Reduza workers: `n_workers=4`
2. Reduza memória: `memory_limit='2GB'`
3. Monitore com: `dask dashboard` em http://127.0.0.1:8787

## 📊 Benchmarks Recomendados

Execute um teste pequeno primeiro:
```python
# Teste com 1 arquivo apenas
# Meça o tempo e uso de CPU
# Ajuste configurações
# Execute dataset completo
```

## 🎯 Configuração Ideal por Hardware

### 8 CPUs, 16GB RAM:
```python
setup_dask_client(n_workers=4, threads_per_worker=2, memory_limit='3GB')
```

### 16 CPUs, 32GB RAM:
```python
setup_dask_client(n_workers=8, threads_per_worker=2, memory_limit='3.5GB')
```

### 32 CPUs, 64GB RAM:
```python
setup_dask_client(n_workers=16, threads_per_worker=2, memory_limit='3.5GB')
```

---

**Nota**: A configuração automática já deve funcionar bem na maioria dos casos. Ajuste manual apenas se necessário!