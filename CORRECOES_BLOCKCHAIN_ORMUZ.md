# Correções de Blockchain — Problema 3 ORMUZ

Este pacote foi ajustado para o barema do Problema 3 considerando a regra de projeto definida pelo grupo:

> Não há companhias nem transferência de créditos entre companhias. Os ativos são créditos operacionais por setor. Sensores de cada setor fazem requisições de escolta; a requisição só é concluída se o setor solicitante tiver saldo suficiente no ledger.

## Principais mudanças feitas

### 1. Blockchain local em cada broker

Cada `Broker` agora mantém uma `localChain` completa (`Blockchain`) e produz blocos localmente. O Hazelcast é usado para:

- replicar blocos entre brokers (`IMap<String, Block> ormuz-shared-blockchain`);
- serializar a criação de blocos com `FencedLock` (`ormuz-blockchain-lock`);
- manter um índice global linear com `IAtomicLong` (`blockchain-global-index`).

Fluxo de criação de bloco:

```text
Broker recebe transação
→ adquire FencedLock CP
→ sincroniza localChain com os blocos distribuídos
→ valida a cadeia local
→ cria o bloco localmente usando o último hash local
→ adiciona o bloco na localChain
→ publica o bloco no Hazelcast
→ demais brokers atualizam suas localChain via EntryListener
```

Com isso, os brokers deixam de depender de uma cadeia local parcial e passam a ter cópias locais equivalentes do ledger. Além da `localChain` em memória, cada broker grava `/data/blockchain.jsonl`, um snapshot completo da cadeia local, e registra no `ledger.log` os eventos `CHAIN_COPY` e `CHAIN_FILE`.

### 2. Saldo derivado da blockchain

O saldo não é mais tratado como fonte de verdade em `sectorBalances`. O mapa continua existindo apenas como cache/visualização. A validação de pagamento usa:

```java
Blockchain.calculateSectorBalance(localChain.getChain(), sector)
```

Eventos que alteram saldo:

- `ISSUE`: emissão inicial do setor;
- `CREDIT`: recarga administrativa;
- `PAYMENT`: pagamento de requisição de escolta.

Eventos que não alteram saldo:

- `GENESIS`;
- `PAYMENT_REJECTED`;
- `MISSION_LOG`.

### 3. Saldos iniciais diferentes por setor

Foram criadas emissões iniciais diferentes, gravadas como blocos `ISSUE`:

| Setor | Saldo inicial |
|---|---:|
| `SETOR_NORTE` | 12 |
| `SETOR_SUL` | 8 |
| `SETOR_LESTE` | 5 |
| `SETOR_OESTE` | 3 |
| `SETOR_TESTE_CARGA` | 40 |

Setores desconhecidos recebem uma emissão inicial padrão de 5 tokens quando aparecem pela primeira vez.

### 4. Prevenção de duplo gasto pelo ledger

O duplo gasto agora é detectado consultando a blockchain:

```java
Blockchain.hasConfirmedPaymentWithToken(localChain.getChain(), tokenId)
```

O `usedTokenIds` permanece apenas como cache derivado da cadeia. Se o mesmo `tokenId` for usado novamente, a requisição é rejeitada e a tentativa é registrada como `PAYMENT_REJECTED`.

### 5. Autenticação das requisições dos sensores

Foi adicionada a classe:

```text
ormuz-shared/src/main/java/com/ormuz/shared/security/SectorAuthenticator.java
```

As mensagens críticas dos sensores passam a carregar:

- `tokenId`;
- `requestTimestamp`;
- `requestSignature`.

A assinatura é calculada sobre setor, nó, token, serviço, dado e timestamp. Para o ambiente didático, a chave vem de `ORMUZ_AUTH_SECRET`; se a variável não existir, é usado um segredo padrão de demonstração.

### 6. Laudos de missão completos no ledger

Os laudos agora são gravados na blockchain como `MISSION_LOG`, contendo:

- setor solicitante;
- serviço;
- drone;
- rota;
- missão;
- resultado (`ROTA_SEGURA`, `OBSTACULO_DETECTADO`, etc.);
- timestamp e hash do bloco.

O `Drone` passou a guardar `missionId`, `routeId` e setor da missão recebidos no comando `ACTIVATE` e os devolve no sinal `BROKER_SIGNAL_COMPLETION`.

### 7. Monitor audita pelo histórico da blockchain

O `AppMonitor` foi atualizado para:

- listar todos os blocos com tipo de transação, origem, destino, quantidade, token, assinatura, missão, drone, rota e laudo;
- validar a cadeia por hash e encadeamento;
- mostrar saldos derivados da blockchain, não de variável local;
- listar apenas tokens de pagamento confirmados no ledger.

### 8. Recarga de saldo também vira bloco

A opção de recarga do painel do broker (`ADD_TOKENS`) agora é processada por um único broker, que cria localmente um bloco `CREDIT` e replica a atualização para a rede. Isso evita que todos os brokers recarreguem o mesmo setor em duplicidade.

### 9. Reatribuição de missão sem nova cobrança

Quando um drone desconecta ou expira por timeout, o broker preserva `missionId`, `routeId`, `pendingPaymentTokenId` e setor no `DroneData`. A tarefa volta para a fila como `REASSIGN_MISSION`. Antes de despachar outro drone, o broker verifica se o `pendingPaymentTokenId` já possui um `PAYMENT` confirmado na blockchain. Assim, a reatribuição aproveita o pagamento original e não cria novo débito.

### 10. Registro de drones não sobrescreve estado ocupado

Reconexões de sockets de drones agora usam `putIfAbsent` e preservam o estado `inUse=true`. Isso evita que um drone em missão seja marcado como livre por uma nova conexão/subscrição e reduza o risco de alocação duplicada.

## Arquivos principais alterados

- `ormuz-shared/src/main/java/com/ormuz/shared/types/Transaction.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/types/Message.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/types/DroneData.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/interfaces/DroneInterface.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/enums/TransactionType.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/enums/CommandType.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/security/SectorAuthenticator.java`
- `ormuz-shared/src/main/java/com/ormuz/shared/blockchain/Blockchain.java`
- `ormuz-broker/src/main/java/com/ormuz/broker/core/Broker.java`
- `ormuz-broker/src/main/java/com/ormuz/broker/app/AppBroker.java`
- `ormuz-broker/src/main/java/com/ormuz/broker/ledger/LedgerWriter.java`
- `ormuz-clients/src/main/java/com/ormuz/client/core/Client.java`
- `ormuz-clients/src/main/java/com/ormuz/client/drone/Drone.java`
- `ormuz-clients/src/main/java/com/ormuz/client/app/AppMonitor.java`

## Como demonstrar no barema

1. Subir múltiplos brokers.
2. Abrir o monitor e usar a opção `6` para listar a blockchain.
3. Usar a opção `8` para verificar que os saldos são derivados da blockchain.
4. Gerar uma requisição instável de sensor; o broker deve criar bloco `PAYMENT` antes de ativar o drone.
5. Aguardar o drone terminar; o broker deve criar bloco `MISSION_LOG`.
6. Usar a opção `7` do monitor para validar a cadeia.
7. Consultar a mesma blockchain por outro monitor/broker e verificar que os blocos e saldos batem.
8. Para testar duplo gasto, enviar duas mensagens críticas com o mesmo `tokenId`; apenas a primeira deve gerar `PAYMENT`, a segunda deve virar `PAYMENT_REJECTED`.
9. Para demonstrar persistência local, abrir `data/dev/broker-*/blockchain.jsonl` e confirmar que cada broker possui a cadeia completa.
10. Para demonstrar reatribuição sem cobrança dupla, pausar ou encerrar um drone durante missão e verificar no ledger que a reatribuição usa `REASSIGN_MISSION` sem criar novo `PAYMENT`.
