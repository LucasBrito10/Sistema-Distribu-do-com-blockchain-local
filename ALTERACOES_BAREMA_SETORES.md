# Alterações aplicadas para o barema — modelo por setores

Este pacote foi ajustado considerando a decisão de projeto: **não há companhias e não há transferência de dinheiro/créditos entre companhias**. O ativo auditado é o saldo operacional de cada setor.

## Correções principais

1. **Cópia local completa da blockchain por broker**
   - Cada broker agora grava `/data/blockchain.jsonl` com o snapshot completo da cadeia.
   - O `ledger.log` registra `CHAIN_COPY` quando um bloco passa a existir localmente e `CHAIN_FILE` quando o snapshot é regravado.
   - Na inicialização, se o cluster estiver vazio e houver `blockchain.jsonl` válido, o broker restaura essa cadeia no mapa distribuído.

2. **Consenso/serialização mais explícitos**
   - O Hazelcast CP Subsystem é configurado para 3 membros CP quando o cluster informado possui 3 ou mais brokers.
   - `FencedLock` continua serializando a criação dos blocos e `IAtomicLong` mantém o índice global linear.

3. **Reatribuição de missão sem nova cobrança**
   - `DroneData` agora guarda `pendingMissionId`, `pendingRouteId`, `pendingPaymentTokenId` e `pendingOriginalRequesterId`.
   - Em timeout/desconexão de drone ou queda de broker, o sistema cria `REASSIGN_MISSION` e valida se o token original já possui `PAYMENT` confirmado no ledger.
   - A missão é reatribuída sem gerar novo `PAYMENT`.

4. **Registro de drone não sobrescreve estado ocupado**
   - `addConnection` passou a usar `putIfAbsent` e preserva `inUse=true` em reconexões.
   - Isso evita que múltiplos sockets do mesmo drone ou reconexões marquem um drone em missão como livre.

5. **Teste do barema ajustado**
   - O teste de duplo gasto agora usa o próprio ID do `AppBaremaAuditTest` nas requisições duplicadas, permitindo receber corretamente `PAYMENT_REJECTED`.

6. **Docker Compose usa build local**
   - `docker-compose.yaml` e `docker-compose.dev.yaml` agora possuem `build:` para broker e clients, evitando executar imagem antiga do Docker Hub.

## Arquivos mais importantes alterados

- `ormuz-broker/src/main/java/com/ormuz/broker/core/Broker.java`
- `ormuz-broker/src/main/java/com/ormuz/broker/ledger/LedgerWriter.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/types/DroneData.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/interfaces/DroneInterface.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/enums/CommandType.java`
- `ormuz-clients/src/main/java/com/ormuz/client/drone/Drone.java`
- `ormuz-clients/src/main/java/com/ormuz/client/app/AppBaremaAuditTest.java`
- `docker-compose.yaml`
- `docker-compose.dev.yaml`
- `README.md`

## Como testar

```bash
docker compose -f docker-compose.dev.yaml up -d --build
docker compose -f docker-compose.dev.yaml --profile admin run --rm teste-barema
```

Depois, confira:

```bash
ls data/dev/broker-*/blockchain.jsonl
cat data/dev/broker-norte/ledger.log | grep -E "CHAIN_COPY|CHAIN_FILE|DEBIT_OK|MISSION_LOG|DOUBLE_SPEND"
```
