# Infraestrutura Distribuída com Blockchain Local para Coordenação de Drones Autônomos

> **TEC502 — Sistemas Distribuídos · UEFS**  
> Problema 3: Economia e Auditoria de Guerra no Estreito de Ormuz

Sistema distribuído descentralizado para coordenação de drones de monitoramento marítimo no Estreito de Ormuz. A solução utiliza **brokers distribuídos**, **Hazelcast**, **sockets TCP**, **ledger local persistente** e **blockchain local replicada** para registrar pagamentos, rejeições, saldos de setores e laudos de missão.

A versão atual elimina a dependência de um servidor central e mantém a auditoria distribuída: cada setor possui seu próprio broker, seus sensores, seus drones, seu ledger local e uma cópia da blockchain. O estado operacional é compartilhado pelo cluster Hazelcast, enquanto os eventos auditáveis são gravados em blocos encadeados por hash.

---

## Arquitetura da Solução

### Estilo Arquitetural

O sistema adota um estilo de **broker distribuído com cluster peer-to-peer entre os setores**. Não existe broker central. Cada máquina do laboratório executa um setor completo:

- `broker`
- `sensores`
- `drones`

Os brokers formam um cluster Hazelcast usando a porta `5701`. Os clientes locais se conectam preferencialmente ao broker da própria máquina pela rede interna do Docker Compose. Em caso de falha, os clientes tentam os demais IPs configurados em `BROKER_IP`.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              CLUSTER HAZELCAST                               │
│                                                                              │
│  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐              │
│  │ Broker NORTE   │◄──►│ Broker SUL     │◄──►│ Broker LESTE   │  ...         │
│  │ 172.16.201.16  │   │ 172.16.201.10  │   │ 172.16.201.11  │              │
│  │ :8080 / :5701  │   │ :8080 / :5701  │   │ :8080 / :5701  │              │
│  └───────┬────────┘   └───────┬────────┘   └───────┬────────┘              │
│          │                    │                    │                       │
│   sensores/drones      sensores/drones      sensores/drones                 │
│                                                                              │
│  ┌────────────────┐                                                        │
│  │ Broker OESTE   │                                                        │
│  │ 172.16.201.7   │                                                        │
│  │ :8080 / :5701  │                                                        │
│  └───────┬────────┘                                                        │
│          │                                                                 │
│   sensores/drones                                                           │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Componentes

| Componente | Módulo | Papel |
|---|---|---|
| **Broker de Setor** | `ormuz-broker` | Recebe conexões TCP de sensores/drones, processa requisições, atualiza estruturas Hazelcast e registra eventos na blockchain |
| **Drone** | `ormuz-clients` / `Drone` | Registra-se no broker local, aguarda despacho, executa missão e envia laudo de conclusão |
| **Sensor** | `ormuz-clients` / `AppSensores` | Gera requisições de serviço para o setor, consumindo saldo operacional via token |
| **Monitor Central** | `ormuz-clients` / `AppMonitor` | Ferramenta administrativa para acompanhar cluster, drones, fila, blockchain e saldos |
| **Teste do Barema** | `ormuz-clients` / `AppBaremaAuditTest` | Executa auditoria automatizada com pagamentos, duplo gasto, saldo insuficiente e laudos |
| **Ledger Local** | `ormuz-broker` / `LedgerWriter` | Arquivo textual persistente `ledger.log` com eventos auditáveis do broker |
| **Blockchain Local** | `ormuz-shared` / `Blockchain`, `Block`, `Transaction` | Cadeia de blocos gravada em `blockchain.jsonl`, validada por hash e replicada entre brokers |
| **Fila Distribuída** | Hazelcast `IQueue` | Guarda requisições pendentes quando não há drones disponíveis |
| **Mapa Distribuído** | Hazelcast `IMap` | Compartilha estado de drones, clientes, blocos e saldos entre os brokers |
| **Bus Global** | Hazelcast `ITopic` | Canal pub/sub usado para comunicação entre brokers |
| **CP Subsystem** | Hazelcast `FencedLock` / `IAtomicLong` | Garante exclusão mútua na criação de blocos e numeração global da blockchain |

### Ausência de Ponto Único de Falha

- Cada setor executa seu próprio broker, sensores e drones.
- O cluster Hazelcast replica estruturas distribuídas entre os brokers.
- Cada broker mantém sua cópia local de `ledger.log` e `blockchain.jsonl`.
- Os clientes usam failover: primeiro tentam o broker local, depois os demais IPs do cluster.
- Se um broker cair, os demais continuam ativos e o Hazelcast remove o membro do cluster.

---

## Blockchain, Ledger e Auditoria

### Ideia Central

O sistema não trabalha com companhias nem transferência de dinheiro entre empresas. O modelo atual considera que **cada setor possui saldo operacional próprio**. Sensores de um setor só conseguem finalizar requisições se o histórico da blockchain indicar saldo suficiente.

A blockchain registra:

| Tipo de transação | Descrição |
|---|---|
| `GENESIS` | Bloco inicial da cadeia |
| `ISSUE` | Emissão inicial de saldo para um setor |
| `CREDIT` | Recarga administrativa de tokens para um setor |
| `PAYMENT` | Pagamento aprovado por uma requisição de serviço |
| `PAYMENT_REJECTED` | Pagamento rejeitado por duplo gasto, saldo insuficiente ou requisição inválida |
| `MISSION_LOG` | Laudo de missão enviado pelo drone após execução |

### Imutabilidade

Cada bloco possui:

- `index`
- `timestamp`
- `transaction`
- `previousHash`
- `hash`

O hash é calculado com SHA-256 a partir dos campos do bloco e da transação. Como cada bloco guarda o `previousHash`, qualquer alteração em um bloco antigo invalida toda a cadeia posterior.

A validação é feita pelo método:

```java
isChainValid()
```

A cadeia é considerada válida apenas se:

1. o bloco gênesis estiver correto;
2. os índices forem sequenciais;
3. cada `hash` corresponder ao conteúdo do bloco;
4. cada `previousHash` apontar para o hash real do bloco anterior.

### Controle de Saldo

O saldo de cada setor é derivado do histórico da blockchain:

- `ISSUE` e `CREDIT` aumentam o saldo;
- `PAYMENT` reduz o saldo;
- `PAYMENT_REJECTED` não altera saldo;
- `MISSION_LOG` não altera saldo.

Assim, o saldo não depende de uma variável isolada em memória. Ele pode ser recalculado a qualquer momento a partir dos blocos.

### Tratamento de Duplo Gasto

Cada requisição de pagamento possui um `tokenId`. Quando um token já foi utilizado em um bloco `PAYMENT`, uma nova tentativa com o mesmo token é rejeitada e registrada como `PAYMENT_REJECTED`.

Nos logs do broker, a rejeição aparece com eventos como:

```text
DUPLO_GASTO
PAYMENT_REJECTED
REJEIÇÃO_NOTIFICADA
```

### Laudo de Missão

Quando um drone finaliza uma missão, ele envia um laudo ao broker. O broker cria um bloco `MISSION_LOG` contendo dados como:

- `missionId`
- `droneId`
- `routeId`
- `missionReport`
- setor de origem
- serviço executado

Exemplos de laudo:

```text
ROTA_SEGURA
OBSTACULO_DETECTADO
```

---

## Protocolo de Comunicação

### Tecnologia

A comunicação entre clientes e brokers usa **sockets TCP raw** na porta `8080`.

A comunicação entre brokers usa **Hazelcast** na porta `5701`.

### Fluxo Simplificado

| Etapa | Descrição |
|---|---|
| 1 | Sensor registra-se no broker do setor |
| 2 | Drone registra-se no broker do setor |
| 3 | Sensor gera requisição de serviço |
| 4 | Broker verifica token, saldo e duplo gasto |
| 5 | Se aprovado, broker cria bloco `PAYMENT` |
| 6 | Broker reserva drone disponível |
| 7 | Drone executa missão |
| 8 | Drone envia laudo |
| 9 | Broker cria bloco `MISSION_LOG` |
| 10 | Ledger e blockchain locais são atualizados |

### Principais Campos de Auditoria

| Campo | Função |
|---|---|
| `tokenId` | Identificador do pagamento; usado contra duplo gasto |
| `fromSector` | Setor que consumiu saldo |
| `toSector` | Destino lógico da transação |
| `amount` | Quantidade de tokens debitada ou creditada |
| `serviceType` | Serviço solicitado pelo sensor |
| `requestNodeId` | Nó que originou a requisição |
| `requestSignature` | Assinatura/identificação lógica da requisição |
| `missionId` | Identificador da missão executada |
| `droneId` | Drone responsável pela missão |
| `routeId` | Rota associada à missão |
| `missionReport` | Resultado da missão |

---

## Concorrência Distribuída

### Criação de Blocos

A criação de blocos é protegida por lock distribuído:

```text
ormuz-blockchain-lock
```

Esse lock impede que dois brokers criem blocos conflitantes ao mesmo tempo.

### Índice Global

A numeração global dos blocos usa o CP Subsystem do Hazelcast:

```text
blockchain-global-index
```

Com isso, evita-se que múltiplos brokers criem blocos com o mesmo índice.

### Drones e Requisições

- O estado dos drones é compartilhado via `IMap`.
- Requisições pendentes usam `IQueue`.
- O `ITopic` distribui eventos entre brokers.
- O despacho é feito de modo a evitar que dois brokers reservem o mesmo drone simultaneamente.

---

## Confiabilidade

### Persistência Local

Cada broker grava seus dados em:

```text
data/SETOR_NOME/broker-ledger/
```

Arquivos principais:

| Arquivo | Descrição |
|---|---|
| `ledger.log` | Log textual de auditoria do broker |
| `blockchain.jsonl` | Blockchain local em formato JSON Lines |

### Recuperação

Ao iniciar, o broker tenta restaurar a blockchain local a partir de `blockchain.jsonl`. Se a cadeia estiver inválida, ela é ignorada para evitar derivar saldo de um histórico adulterado.

### Falhas Tratadas

| Falha | Tratamento |
|---|---|
| Drone indisponível | Requisição permanece na fila distribuída |
| Broker removido do cluster | Hazelcast dispara evento de membro removido |
| Token já usado | Transação rejeitada por duplo gasto |
| Saldo insuficiente | Transação rejeitada sem alterar saldo |
| Blockchain local inválida | Arquivo é ignorado na inicialização |

---

## Execução com Docker

### Pré-requisitos

Em todas as máquinas:

```bash
docker --version
docker compose version
```

Instale `jq` para visualizar a blockchain formatada:

```bash
sudo apt update
sudo apt install -y jq netcat-openbsd
```

Libere as portas necessárias:

```bash
sudo ufw allow 8080/tcp
sudo ufw allow 5701/tcp
sudo ufw reload
sudo ufw status
```

### Imagens Docker

O `docker-compose.yaml` utiliza duas imagens:

```text
lucasbrito007/ormuz-broker:latest
lucasbrito007/ormuz-clients:latest
```

Para fazer build local:

```bash
docker compose -f docker-compose.yaml build
```

Para build limpo:

```bash
docker compose -f docker-compose.yaml build --no-cache
```

Para enviar ao Docker Hub:

```bash
docker login
docker push lucasbrito007/ormuz-broker:latest
docker push lucasbrito007/ormuz-clients:latest
```

Para baixar em outra máquina:

```bash
docker pull lucasbrito007/ormuz-broker:latest
docker pull lucasbrito007/ormuz-clients:latest
```

---

## Execução Multi-Máquina

### IPs Utilizados

| Setor | IP físico |
|---|---|
| `SETOR_NORTE` | `172.16.201.16` |
| `SETOR_SUL` | `172.16.201.10` |
| `SETOR_LESTE` | `172.16.201.11` |
| `SETOR_OESTE` | `172.16.201.7` |

O `CLUSTER_IPS` deve ser igual em todas as máquinas:

```text
172.16.201.16,172.16.201.10,172.16.201.11,172.16.201.7
```

### Máquina Norte

```bash
cd ~/Downloads/s2

cat > .env <<'EOF'
SETOR_NOME=SETOR_NORTE
PUBLIC_ADDRESS=172.16.201.16
CLUSTER_IPS=172.16.201.16,172.16.201.10,172.16.201.11,172.16.201.7
TEST_SECTORS=SETOR_NORTE,SETOR_SUL,SETOR_LESTE,SETOR_OESTE
EOF

docker compose -f docker-compose.yaml --profile admin down --remove-orphans
docker rm -f broker-SETOR_NORTE sensores-SETOR_NORTE drones-SETOR_NORTE monitor-central 2>/dev/null || true

rm -rf data/SETOR_NORTE
mkdir -p data/SETOR_NORTE/broker-ledger

docker compose -f docker-compose.yaml build

docker compose -f docker-compose.yaml up -d --force-recreate broker sensores drones

docker ps
```

### Máquina Sul

```bash
cd ~/Downloads/s2

cat > .env <<'EOF'
SETOR_NOME=SETOR_SUL
PUBLIC_ADDRESS=172.16.201.10
CLUSTER_IPS=172.16.201.16,172.16.201.10,172.16.201.11,172.16.201.7
TEST_SECTORS=SETOR_NORTE,SETOR_SUL,SETOR_LESTE,SETOR_OESTE
EOF

docker compose -f docker-compose.yaml --profile admin down --remove-orphans
docker rm -f broker-SETOR_SUL sensores-SETOR_SUL drones-SETOR_SUL 2>/dev/null || true

rm -rf data/SETOR_SUL
mkdir -p data/SETOR_SUL/broker-ledger

docker compose -f docker-compose.yaml build

docker compose -f docker-compose.yaml up -d --force-recreate broker sensores drones

docker ps
```

### Máquina Leste

```bash
cd ~/Downloads/s2

cat > .env <<'EOF'
SETOR_NOME=SETOR_LESTE
PUBLIC_ADDRESS=172.16.201.11
CLUSTER_IPS=172.16.201.16,172.16.201.10,172.16.201.11,172.16.201.7
TEST_SECTORS=SETOR_NORTE,SETOR_SUL,SETOR_LESTE,SETOR_OESTE
EOF

docker compose -f docker-compose.yaml --profile admin down --remove-orphans
docker rm -f broker-SETOR_LESTE sensores-SETOR_LESTE drones-SETOR_LESTE 2>/dev/null || true

rm -rf data/SETOR_LESTE
mkdir -p data/SETOR_LESTE/broker-ledger

docker compose -f docker-compose.yaml build

docker compose -f docker-compose.yaml up -d --force-recreate broker sensores drones

docker ps
```

### Máquina Oeste

```bash
cd ~/Downloads/s2

cat > .env <<'EOF'
SETOR_NOME=SETOR_OESTE
PUBLIC_ADDRESS=172.16.201.7
CLUSTER_IPS=172.16.201.16,172.16.201.10,172.16.201.11,172.16.201.7
TEST_SECTORS=SETOR_NORTE,SETOR_SUL,SETOR_LESTE,SETOR_OESTE
EOF

docker compose -f docker-compose.yaml --profile admin down --remove-orphans
docker rm -f broker-SETOR_OESTE sensores-SETOR_OESTE drones-SETOR_OESTE 2>/dev/null || true

rm -rf data/SETOR_OESTE
mkdir -p data/SETOR_OESTE/broker-ledger

docker compose -f docker-compose.yaml build

docker compose -f docker-compose.yaml up -d --force-recreate broker sensores drones

docker ps
```

### Subida Interativa

Quando for necessário acompanhar os logs diretamente no terminal, execute sem `-d`:

```bash
docker compose -f docker-compose.yaml up broker sensores drones
```

Para subir somente o broker interativo:

```bash
docker compose -f docker-compose.yaml up broker
```

Para subir sensores e drones em outro terminal:

```bash
docker compose -f docker-compose.yaml up sensores drones
```

---

## Monitor Central

O monitor deve ser executado apenas em uma máquina administrativa, preferencialmente a Norte.

```bash
cd ~/Downloads/s2

docker rm -f monitor-central 2>/dev/null || true

docker compose -f docker-compose.yaml --profile admin up -d --force-recreate monitor-central

docker logs -f monitor-central
```

Para entrar de forma interativa:

```bash
docker attach monitor-central
```

Para sair do `attach` sem parar o container:

```text
CTRL + P
CTRL + Q
```

Se aparecer conflito de nome:

```bash
docker rm -f monitor-central
docker compose -f docker-compose.yaml --profile admin up monitor-central
```

---

## Testes

### Teste Principal do Barema

Execute apenas na máquina administrativa:

```bash
cd ~/Downloads/s2

set -a
source .env
set +a

mkdir -p auditoria

docker compose -f docker-compose.yaml --profile admin run --rm -it \
  -e TEST_REQUESTS=12 \
  -e TEST_CREDIT_AMOUNT=1 \
  -e TEST_WAIT_SECONDS=80 \
  -e TEST_DOUBLE_SPEND=true \
  -e TEST_BALANCE_DOUBLE_SPEND=true \
  teste-barema | tee auditoria/teste-barema-forte.txt
```

Esse teste verifica:

- emissão e recarga de tokens;
- requisições de sensores;
- débitos aprovados;
- rejeição por saldo insuficiente;
- rejeição de duplo gasto;
- registro de laudos de missão;
- saldos derivados da blockchain;
- validade da cadeia.

### Teste Simples

```bash
docker compose -f docker-compose.yaml --profile admin run --rm -it \
  -e TEST_REQUESTS=4 \
  -e TEST_CREDIT_AMOUNT=1 \
  -e TEST_WAIT_SECONDS=40 \
  -e TEST_DOUBLE_SPEND=false \
  -e TEST_BALANCE_DOUBLE_SPEND=false \
  teste-barema | tee auditoria/teste-barema-simples.txt
```

### Teste de Duplo Gasto

```bash
docker compose -f docker-compose.yaml --profile admin run --rm -it \
  -e TEST_REQUESTS=8 \
  -e TEST_CREDIT_AMOUNT=1 \
  -e TEST_WAIT_SECONDS=60 \
  -e TEST_DOUBLE_SPEND=true \
  -e TEST_BALANCE_DOUBLE_SPEND=false \
  teste-barema | tee auditoria/teste-duplo-gasto.txt
```

### Teste de Saldo Insuficiente

```bash
docker compose -f docker-compose.yaml --profile admin run --rm -it \
  -e TEST_REQUESTS=20 \
  -e TEST_CREDIT_AMOUNT=1 \
  -e TEST_WAIT_SECONDS=100 \
  -e TEST_DOUBLE_SPEND=false \
  -e TEST_BALANCE_DOUBLE_SPEND=true \
  teste-barema | tee auditoria/teste-saldo-insuficiente.txt
```

---

## Verificação dos Logs

### Logs do Broker

```bash
set -a
source .env
set +a

docker logs -f broker-$SETOR_NOME
```

### Eventos Importantes

```bash
docker logs broker-$SETOR_NOME | grep -E "clusterSize|MEMBRO_ADICIONADO|MEMBRO_REMOVIDO|DÉBITO_APROVADO|DÉBITO_REJEITADO|DUPLO_GASTO|SALDO_INSUFICIENTE|LAUDO_REGISTRADO|RECARGA_REGISTRADA|BLOCKCHAIN"
```

### Verificar Cluster

```bash
docker logs broker-$SETOR_NOME | grep -E "HAZELCAST_INICIADO|clusterSize|MEMBRO_ADICIONADO|MEMBRO_REMOVIDO|Members|Member \[|PUBLIC_ADDRESS"
```

O resultado esperado é que o broker não apareça como `127.0.0.1`. Em multi-máquina, o Hazelcast deve anunciar o IP físico da máquina.

Correto:

```text
Member [172.16.201.16]:5701
```

Incorreto:

```text
Member [127.0.0.1]:5701
```

Se aparecer `127.0.0.1`, revise a variável:

```bash
cat .env
```

E confira o Compose resolvido:

```bash
docker compose -f docker-compose.yaml config | grep -E "SETOR_NOME|PUBLIC_ADDRESS|CLUSTER_IPS|5701|8080" -n
```

### Teste de Conectividade

```bash
nc -vz -w 3 172.16.201.16 5701
nc -vz -w 3 172.16.201.10 5701
nc -vz -w 3 172.16.201.11 5701
nc -vz -w 3 172.16.201.7 5701
```

---

## Verificação do Ledger e da Blockchain

### Carregar variáveis

```bash
set -a
source .env
set +a
```

### Ver Ledger

```bash
cat data/$SETOR_NOME/broker-ledger/ledger.log
```

### Ver Blockchain

```bash
cat data/$SETOR_NOME/broker-ledger/blockchain.jsonl | jq '.'
```

### Ver Pagamentos

```bash
grep -i "PAYMENT" data/$SETOR_NOME/broker-ledger/blockchain.jsonl
```

### Ver Rejeições

```bash
grep -i "PAYMENT_REJECTED\|DUPLO\|SALDO" data/$SETOR_NOME/broker-ledger/ledger.log
```

### Ver Laudos

```bash
grep -i "MISSION_LOG\|ROTA_SEGURA\|OBSTACULO" data/$SETOR_NOME/broker-ledger/blockchain.jsonl
```

---

## Ambiente de Desenvolvimento Local

Para simular os quatro setores em uma única máquina:

```bash
docker compose -f docker-compose.dev.yaml up -d --build
```

Para incluir o monitor:

```bash
docker compose -f docker-compose.dev.yaml --profile admin up -d
docker logs -f monitor-central
```

Para parar:

```bash
docker compose -f docker-compose.dev.yaml --profile admin down --remove-orphans
```

---

## Estrutura do Repositório

```
.
├── docker-compose.yaml              # Deploy multi-máquina
├── docker-compose.dev.yaml          # Deploy local simulando 4 setores
├── ormuz-broker/                    # Módulo do broker de setor
│   └── src/main/java/com/ormuz/broker/
│       ├── app/AppBroker.java
│       ├── core/Broker.java
│       ├── core/BrokerBuilder.java
│       ├── ledger/LedgerWriter.java
│       ├── log/BrokerLogger.java
│       └── network/MiddlewareHandlerServer.java
├── ormuz-clients/                   # Módulo de drones, sensores e ferramentas
│   └── src/main/java/com/ormuz/client/
│       ├── app/AppFrotaDrones.java
│       ├── app/AppSensores.java
│       ├── app/AppMonitor.java
│       ├── app/AppLoadTest.java
│       ├── app/AppBaremaAuditTest.java
│       ├── core/Client.java
│       ├── core/MiddlewareClient.java
│       ├── drone/Drone.java
│       ├── sensor/CoastalRadar.java
│       ├── sensor/NavalSensor.java
│       └── buoy/SmartBuoy.java
└── ormuz-shared/                    # Tipos compartilhados
    └── src/main/java/com/ormuz/shared/
        ├── blockchain/Blockchain.java
        ├── enums/CommandType.java
        ├── enums/ServicesTypes.java
        ├── enums/ResourcesTypes.java
        ├── enums/TopicType.java
        ├── enums/TransactionType.java
        ├── security/SectorAuthenticator.java
        └── types/
            ├── Block.java
            ├── Transaction.java
            ├── Message.java
            ├── DroneData.java
            └── ClientData.java
```

---

## Comandos de Apresentação

### Mostrar containers ativos

```bash
docker ps
```

### Mostrar cluster

```bash
docker logs broker-$SETOR_NOME | grep -E "clusterSize|MEMBRO_ADICIONADO|Members|Member \["
```

### Mostrar pagamento aprovado

```bash
docker logs broker-$SETOR_NOME | grep "DÉBITO_APROVADO"
```

### Mostrar duplo gasto rejeitado

```bash
docker logs broker-$SETOR_NOME | grep -E "DUPLO_GASTO|PAYMENT_REJECTED"
```

### Mostrar saldo insuficiente

```bash
docker logs broker-$SETOR_NOME | grep -E "SALDO_INSUFICIENTE|DÉBITO_REJEITADO"
```

### Mostrar laudo de missão

```bash
docker logs broker-$SETOR_NOME | grep "LAUDO_REGISTRADO"
```

### Mostrar blockchain formatada

```bash
cat data/$SETOR_NOME/broker-ledger/blockchain.jsonl | jq '.'
```

### Mostrar ledger

```bash
cat data/$SETOR_NOME/broker-ledger/ledger.log
```

---

## Parar o Sistema

Em cada máquina:

```bash
docker compose -f docker-compose.yaml --profile admin down --remove-orphans
```

Para apagar dados locais:

```bash
rm -rf data
```

Para remover conflitos de nomes:

```bash
docker rm -f monitor-central 2>/dev/null || true
docker rm -f broker-SETOR_NORTE sensores-SETOR_NORTE drones-SETOR_NORTE 2>/dev/null || true
docker rm -f broker-SETOR_SUL sensores-SETOR_SUL drones-SETOR_SUL 2>/dev/null || true
docker rm -f broker-SETOR_LESTE sensores-SETOR_LESTE drones-SETOR_LESTE 2>/dev/null || true
docker rm -f broker-SETOR_OESTE sensores-SETOR_OESTE drones-SETOR_OESTE 2>/dev/null || true
```

---

## Tecnologias Utilizadas

| Tecnologia | Uso |
|---|---|
| **Java 17+** | Linguagem principal |
| **Maven** | Build e gerenciamento de dependências |
| **Hazelcast 5.3.6** | Cluster distribuído, mapas, fila, tópicos e CP Subsystem |
| **Sockets TCP** | Comunicação entre clientes e brokers |
| **SHA-256** | Hash dos blocos da blockchain |
| **Docker / Docker Compose** | Contêinerização e execução multi-máquina |
| **JSON Lines** | Persistência da blockchain local em `blockchain.jsonl` |

---

## Informações Acadêmicas

- **Disciplina**: TEC502 — Sistemas Distribuídos
- **Instituição**: Universidade Estadual de Feira de Santana (UEFS)
- **Departamento**: Tecnologia
- **Laboratórios de apresentação**: LARSID / LADICA
- **Projeto**: Sistema ORMUZ com blockchain local, ledger distribuído e auditoria de duplo gasto
