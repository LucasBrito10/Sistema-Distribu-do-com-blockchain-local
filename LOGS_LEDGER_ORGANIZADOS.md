# Organização dos logs e do ledger — ORMUZ

## O que foi padronizado

Esta atualização reorganiza as saídas de terminal e o arquivo `ledger.log` para facilitar a leitura durante os testes do barema, principalmente quando vários brokers, sensores e drones estão executando ao mesmo tempo.

## Terminal dos brokers

Os logs do broker agora seguem o formato:

```text
2026-06-15 16:42:10.135 | INFO  | BLOCKCHAIN  | DÉBITO_APROVADO | bloco=#12 | setor=SETOR_NORTE | saldo=12 -> 11 | serviço=INTRUSION_DETECTION
```

Campos principais:

- `timestamp`: data/hora com milissegundos.
- `nível`: `INFO`, `WARN` ou `ERRO`.
- `área`: `SISTEMA`, `CONEXAO`, `CLUSTER`, `BUS`, `DRONE`, `ALERTA`, `BLOCKCHAIN`, etc.
- `evento`: nome curto e estável do evento.
- `campos chave=valor`: dados objetivos para auditoria.

Principais eventos novos/reorganizados:

- `HAZELCAST_INICIADO`
- `ESTRUTURAS_INICIALIZADAS`
- `DÉBITO_APROVADO`
- `DUPLO_GASTO`
- `SALDO_INSUFICIENTE`
- `RECARGA_REGISTRADA`
- `LAUDO_REGISTRADO`
- `REQUISIÇÃO_ENFILEIRADA`
- `TIMEOUT_DE_DRONE`
- `MISSAO_REATRIBUIDA_SEM_DEBITO`
- `CHAIN_COPY` / `CHAIN_FILE`

## Terminal dos clientes, sensores, drones e monitor

Foi adicionado o utilitário compartilhado:

```text
ormuz-shared/src/main/java/com/ormuz/shared/util/ConsoleLog.java
```

Ele padroniza linhas de terminal em clientes, sensores, drones e monitor. Exemplo:

```text
2026-06-15 16:43:01.022 | WARN  | SENSOR         | LEITURA_INSTÁVEL       | setor=SETOR_NORTE | nó=RADAR_COSTEIRO | dado=INTRUSION_DETECTION | motivo=Embarcação hostil detectada!
2026-06-15 16:43:16.041 | INFO  | DRONE          | LAUDO_ENVIADO          | drone=DRN-DINAMICO-1-SETOR_NORTE | missão=MIS-12-DRN-DINAMICO-1-SETOR_NORTE | rota=ROTA-SETOR_NORTE-DRN-DINAMICO-1-SETOR_NORTE | laudo=ROTA_SEGURA
```

## Monitor

O `AppMonitor` foi reorganizado para exibir:

- menu mais claro;
- tabelas de drones/equipamentos;
- blockchain com blocos separados e campos indentados;
- saldos em tabela;
- validação da cadeia com resumo de auditoria.

Também foi corrigido um `if (saldos.isEmpty())` duplicado no método de listagem de saldos.

## Ledger

Além do `ledger.log`, cada broker mantém uma cópia completa da blockchain em `blockchain.jsonl` no mesmo volume `/data`. O `ledger.log` continua sendo o arquivo humano/legível; o `blockchain.jsonl` é o snapshot local completo usado para auditoria e restauração.

O `LedgerWriter` agora grava eventos em blocos de leitura mais clara:

```text
2026-06-15 16:57:10 | DEBIT_OK      | bloco=#00007 | setor=SETOR_NORTE | saldo=11
    serviço     : INTRUSION_DETECTION
    token       : REQ-1234567890
    hash        : abcdefabcdefabcdefabcdef...
    anterior    : 000000000000000000000000...
2026-06-15 16:57:10 | MISSION_LOG   | bloco=#00008 | setor=SETOR_NORTE | drone=DRN-1
    serviço     : VISUAL_RECONNAISSANCE
    rota        : ROTA-1
    laudo       : ROTA_SEGURA
    hash        : 123451234512345123451234...
    anterior    : abcdefabcdefabcdefabcdef...
```

Eventos registrados:

- `STARTUP`
- `DEBIT_OK`
- `DOUBLE_SPEND`
- `NO_FUNDS`
- `CAS_FAIL`
- `CREDIT`
- `MISSION_LOG`
- `CHAIN_COPY`: bloco que este broker passou a possuir localmente, inclusive quando recebido de outro broker
- `CHAIN_FILE`: snapshot completo da blockchain local regravado em `blockchain.jsonl`
- `SHUTDOWN`

## Como filtrar logs do broker

A variável `BROKER_LOG_TYPES` continua funcionando. Exemplo:

```bash
BROKER_LOG_TYPES=SISTEMA,BLOCKCHAIN,ALERTA,DRONE
```

Isso mantém visíveis apenas os logs mais importantes para auditoria.
