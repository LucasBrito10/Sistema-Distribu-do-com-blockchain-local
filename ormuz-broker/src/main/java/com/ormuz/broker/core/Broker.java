package com.ormuz.broker.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.topic.ITopic;
import com.ormuz.broker.ledger.LedgerWriter;
import com.ormuz.broker.log.BrokerLogger;
import com.ormuz.broker.log.BrokerLogger.LogType;
import com.ormuz.broker.network.MiddlewareHandlerBuilder;
import com.ormuz.broker.network.MiddlewareHandlerInterface;
import com.ormuz.shared.blockchain.Blockchain;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.enums.TopicType;
import com.ormuz.shared.enums.TransactionType;
import com.ormuz.shared.interfaces.DroneInterface;
import com.ormuz.shared.security.SectorAuthenticator;
import com.ormuz.shared.types.Block;
import com.ormuz.shared.types.ClientData;
import com.ormuz.shared.types.DroneData;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.types.Transaction;

public class Broker implements BrokerInterface {
    private ServerSocket serverSocket;
    private final Map<TopicType, Set<MiddlewareHandlerInterface>> clientsPerTopic = new ConcurrentHashMap<>();
    private final Map<String, MiddlewareHandlerInterface> clientsById = new ConcurrentHashMap<>();
    /**
     * Um mesmo nó pode abrir várias conexões (publisher/subscriber e tópicos diferentes).
     * O broker só deve considerar o nó desconectado quando todas elas caírem.
     */
    private final Map<String, Set<MiddlewareHandlerInterface>> connectionsById = new ConcurrentHashMap<>();

    private IMap<String, ClientData>     sharedClients;
    private MultiMap<String, String>     sharedTopics;
    private IMap<String, DroneInterface> sharedDrones;
    private ITopic<Message>              clusterWideBus;
    private IQueue<Message>              filaRequisicoes;

    /** Caches distribuídos derivados da blockchain. Não são fonte de verdade. */
    private ISet<String>          usedTokenIds;
    private IMap<String, Integer> sectorBalances;
    private IMap<String, Block>   sharedBlockchain;

    /** Cada broker mantém uma cópia local completa da cadeia. */
    private final Blockchain      localChain = new Blockchain();
    private LedgerWriter          ledger;
    /** Hashes já materializados no ledger.log deste broker, para evitar duplicação. */
    private final Set<String> persistedBlockHashes = ConcurrentHashMap.newKeySet();
    /** Arquivo local com a cadeia completa em JSONL; um arquivo por broker. */
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private Path blockchainFile;
    /** Evita escrita simultânea do snapshot local por listeners e produção local. */
    private final Object blockchainFileLock = new Object();

    private static final String BLOCKCHAIN_LOCK_NAME = "ormuz-blockchain-lock";
    private static final String BLOCKCHAIN_INDEX_NAME = "blockchain-global-index";

    /** Saldos iniciais diferentes por setor, materializados como blocos ISSUE. */
    private static final Map<String, Integer> INITIAL_SECTOR_BALANCES = createInitialSectorBalances();

    private static Map<String, Integer> createInitialSectorBalances() {
        Map<String, Integer> balances = new LinkedHashMap<>();
        balances.put("SETOR_NORTE", 12);
        balances.put("SETOR_SUL", 8);
        balances.put("SETOR_LESTE", 5);
        balances.put("SETOR_OESTE", 3);
        balances.put("SETOR_TESTE_CARGA", 40);
        return balances;
    }

    private HazelcastInstance hz;
    private String brokerId;

    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
    private static final long TASK_TIMEOUT_MS = 30_000;

    private enum DebitFailureReason {
        INVALID_REQUEST("Requisição inválida ou não autenticada"),
        DOUBLE_SPEND("Duplo gasto detectado: token já utilizado no ledger"),
        INSUFFICIENT_BALANCE("Saldo de tokens insuficiente no setor"),
        CHAIN_INVALID("Blockchain distribuída inválida/adulterada"),
        CONSENSUS_FAILURE("Falha de consenso ao gravar bloco");

        private final String description;

        DebitFailureReason(String description) {
            this.description = description;
        }
    }

    private static final class DebitResult {
        private final boolean approved;
        private final DebitFailureReason failureReason;
        private final int newBalance;
        private final Block block;

        private DebitResult(boolean approved, DebitFailureReason failureReason, int newBalance, Block block) {
            this.approved = approved;
            this.failureReason = failureReason;
            this.newBalance = newBalance;
            this.block = block;
        }

        static DebitResult approved(int newBalance, Block block) {
            return new DebitResult(true, null, newBalance, block);
        }

        static DebitResult rejected(DebitFailureReason reason) {
            return new DebitResult(false, reason, -1, null);
        }

        String reasonText() {
            return failureReason == null ? "" : failureReason.description;
        }
    }


    private static int resolveCpMemberCount(String[] clusterIps) {
        String env = System.getenv("ORMUZ_CP_MEMBER_COUNT");
        if (env != null && !env.isBlank()) {
            try {
                return Integer.parseInt(env.trim());
            } catch (NumberFormatException ignored) {
                return 3;
            }
        }
        if (clusterIps == null) return 0;
        int count = 0;
        for (String ip : clusterIps) {
            if (ip != null && !ip.isBlank()) count++;
        }
        return count >= 3 ? 3 : 0;
    }

    public Broker(int port, String[] clusterIps) {
        try {
            this.serverSocket = new ServerSocket(port);

            Config config = new Config();
            config.setClusterName("ormuz-cluster");
            // Consenso forte para FencedLock/IAtomicLong: Hazelcast CP usa Raft.
            // Em demonstração com 4 brokers, 3 membros CP formam quórum. Em execução
            // isolada, mantém o modo padrão para não bloquear testes unitários locais.
            int cpMemberCount = resolveCpMemberCount(clusterIps);
            if (cpMemberCount >= 3) {
                config.getCPSubsystemConfig().setCPMemberCount(cpMemberCount).setGroupSize(3);
            }

            NetworkConfig network = config.getNetworkConfig();
            // Em execução Docker multi-máquina, a porta publicada pelo Compose é 5701.
            // Desabilitar auto-incremento evita que o Hazelcast suba em 5702/5703
            // silenciosamente e fique inacessível para os demais hosts.
            network.setPort(5701).setPortAutoIncrement(false);
            network.getInterfaces().setEnabled(false);

            String publicAddress = System.getenv("PUBLIC_ADDRESS");
            if (publicAddress != null && !publicAddress.isBlank()) {
                network.setPublicAddress(publicAddress.trim() + ":5701");
                BrokerLogger.event(LogType.SISTEMA, "PUBLIC_ADDRESS", "endereço", publicAddress.trim() + ":5701");
            }

            JoinConfig join = network.getJoin();
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);

            if (clusterIps != null && clusterIps.length > 0) {
                for (String ip : clusterIps) {
                    if (ip != null && !ip.isBlank()) {
                        join.getTcpIpConfig().addMember(ip.trim() + ":5701");
                    }
                }
            } else {
                join.getTcpIpConfig().addMember("127.0.0.1:5701");
            }

            this.hz = Hazelcast.newHazelcastInstance(config);
            this.brokerId = hz.getCluster().getLocalMember().getUuid().toString();
            BrokerLogger.event(LogType.SISTEMA, "HAZELCAST_INICIADO", "broker", brokerId.substring(0, 8), "clusterSize", String.valueOf(hz.getCluster().getMembers().size()));
            initializeAuditPersistence();

            this.clusterWideBus = hz.getTopic("ormuz-global-bus");
            this.clusterWideBus.addMessageListener(msg -> {
                Message incoming = msg.getMessageObject();
                BrokerLogger.event(LogType.BUS, "MENSAGEM_RECEBIDA", "broker", brokerId.substring(0, 8), "origem", incoming != null ? incoming.getNodeId() : "N/A", "destino", incoming != null ? incoming.getTargetNodeId() : "N/A", "serviço", incoming != null && incoming.getServiceType() != null ? incoming.getServiceType().name() : "N/A");
                deliverLocal(incoming);
            });

            this.hz.getCluster().addMembershipListener(new MembershipListener() {
                @Override public void memberAdded(MembershipEvent e) {
                    BrokerLogger.event(LogType.CLUSTER, "MEMBRO_ADICIONADO", "uuid", e.getMember().getUuid().toString());
                    syncLocalChainFromDistributed();
                }
                @Override public void memberRemoved(MembershipEvent e) {
                    BrokerLogger.warn(LogType.CLUSTER, "MEMBRO_REMOVIDO", "uuid", e.getMember().getUuid().toString(), "ação", "limpando dados órfãos");
                    cleanUpOrphanedData(e.getMember().getUuid().toString());
                    syncLocalChainFromDistributed();
                }
            });

            this.sharedClients     = hz.getMap("global-clients-map");
            this.sharedTopics      = hz.getMultiMap("global-topics-multimap");
            this.sharedDrones      = hz.getMap("DroneMap");
            this.sharedDrones.addIndex(IndexType.HASH, "inUse");
            this.filaRequisicoes   = hz.getQueue("ormuz-fila-requisicoes");

            this.usedTokenIds      = hz.getSet("ormuz-used-tokens");
            this.sectorBalances    = hz.getMap("ormuz-sector-balances");
            this.sharedBlockchain  = hz.getMap("ormuz-shared-blockchain");

            attachBlockchainReplicationListeners();
            initializeBlockchainState();

            BrokerLogger.event(LogType.BLOCKCHAIN, "ESTRUTURAS_INICIALIZADAS", "blocosLocais", String.valueOf(localChain.size()), "cadeiaVálida", String.valueOf(localChain.isChainValid()));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (ledger != null) ledger.close();
            }, "ledger-shutdown"));

            this.watchdog.scheduleAtFixedRate(() -> {
                long now = System.currentTimeMillis();
                for (Map.Entry<String, DroneInterface> entry : this.sharedDrones.entrySet()) {
                    DroneInterface drone = entry.getValue();
                    if (!drone.isInUse()) continue;
                    Long activatedAt = drone.getActivatedAt();
                    if (activatedAt != null && (now - activatedAt) > TASK_TIMEOUT_MS) {
                        BrokerLogger.warn(LogType.DRONE, "TIMEOUT_DE_DRONE", "drone", drone.getDroneId(), "ação", "reatribuindo missão já paga sem novo débito");
                        DroneInterface freed = clearMissionState(drone);
                        freed.setInUse(false);
                        if (this.sharedDrones.replace(entry.getKey(), drone, freed)) {
                            requeuePaidMission(drone, "TIMEOUT_DE_DRONE");
                            checkDistributedQueue();
                        }
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);

        } catch (IOException e) {
            BrokerLogger.err(LogType.SISTEMA, "Falha ao iniciar broker: " + e.getMessage());
        }
    }


    private void initializeAuditPersistence() {
        try {
            this.ledger = new LedgerWriter(this.brokerId);
            this.ledger.recordStartup(this.brokerId);
            BrokerLogger.event(LogType.BLOCKCHAIN, "LEDGER_ABERTO", "arquivo", this.ledger.getFilePath(), "status", "pronto para auditoria");
        } catch (IOException e) {
            BrokerLogger.warn(LogType.ALERTA, "LEDGER_INDISPONÍVEL", "erro", e.getMessage());
            this.ledger = null;
        }

        try {
            this.blockchainFile = resolveBlockchainFilePath();
            if (this.blockchainFile.getParent() != null) {
                Files.createDirectories(this.blockchainFile.getParent());
            }
            if (!Files.exists(this.blockchainFile)) {
                Files.createFile(this.blockchainFile);
            }
            BrokerLogger.event(LogType.BLOCKCHAIN, "ARQUIVO_BLOCKCHAIN_LOCAL", "arquivo", this.blockchainFile.toString());
        } catch (IOException e) {
            BrokerLogger.warn(LogType.ALERTA, "BLOCKCHAIN_LOCAL_INDISPONÍVEL", "erro", e.getMessage());
            this.blockchainFile = null;
        }
    }

    private Path resolveBlockchainFilePath() {
        String explicit = System.getenv("BLOCKCHAIN_FILE");
        if (explicit != null && !explicit.isBlank()) {
            return Paths.get(explicit.trim());
        }
        String ledgerPath = System.getenv("LEDGER_FILE");
        if (ledgerPath != null && !ledgerPath.isBlank()) {
            Path parent = Paths.get(ledgerPath.trim()).getParent();
            if (parent != null) {
                return parent.resolve("blockchain.jsonl");
            }
        }
        String shortId = brokerId != null && brokerId.length() >= 8 ? brokerId.substring(0, 8) : "UNKNOWN";
        return Paths.get("blockchain-" + shortId + ".jsonl");
    }

    /** Deve ser chamado com o FencedLock da blockchain já adquirido. */
    private void loadPersistentBlockchainIntoDistributedIfUsefulLocked() {
        List<Block> loaded = loadPersistentBlockchainFromDisk();
        if (loaded.isEmpty()) return;

        List<Block> distributed = Blockchain.orderedCopy(this.sharedBlockchain != null ? this.sharedBlockchain.values() : List.of());
        boolean shouldPublish = distributed.isEmpty()
                || (loaded.size() > distributed.size() && isPrefixChain(distributed, loaded));

        if (!shouldPublish) return;

        for (Block block : loaded) {
            if (block.getHash() != null) {
                this.sharedBlockchain.put(block.getHash(), block);
            }
        }
        this.localChain.replaceChain(loaded);
        alignGlobalIndexWithLocalChainLocked();
        persistLocalChainSnapshot("RESTAURADO_DISCO_LOCAL");
        BrokerLogger.event(LogType.BLOCKCHAIN, "BLOCKCHAIN_RESTAURADA", "arquivo", blockchainFile.toString(), "blocos", String.valueOf(loaded.size()));
    }

    private List<Block> loadPersistentBlockchainFromDisk() {
        if (this.blockchainFile == null || !Files.exists(this.blockchainFile)) return List.of();
        List<Block> blocks = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(this.blockchainFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;
                blocks.add(jsonMapper.readValue(trimmed, Block.class));
            }
        } catch (IOException e) {
            BrokerLogger.warn(LogType.ALERTA, "BLOCKCHAIN_LOCAL_NAO_LIDA", "arquivo", this.blockchainFile.toString(), "erro", e.getMessage());
            return List.of();
        }

        List<Block> ordered = Blockchain.orderedCopy(blocks);
        if (ordered.isEmpty()) return List.of();
        if (!Blockchain.isChainValid(ordered)) {
            BrokerLogger.warn(LogType.ALERTA, "BLOCKCHAIN_LOCAL_INVALIDA", "arquivo", this.blockchainFile.toString(), "ação", "ignorada na inicialização");
            return List.of();
        }
        return ordered;
    }

    private boolean isPrefixChain(List<Block> prefix, List<Block> full) {
        List<Block> orderedPrefix = Blockchain.orderedCopy(prefix);
        List<Block> orderedFull = Blockchain.orderedCopy(full);
        if (orderedPrefix.size() > orderedFull.size()) return false;
        for (int i = 0; i < orderedPrefix.size(); i++) {
            String a = orderedPrefix.get(i).getHash();
            String b = orderedFull.get(i).getHash();
            if (a == null || !a.equals(b)) return false;
        }
        return true;
    }

    private void persistLocalChainSnapshot(String origin) {
        if (this.blockchainFile == null) return;
        synchronized (blockchainFileLock) {
            List<Block> snapshot = localChain.getChain();
            boolean valid = Blockchain.isChainValid(snapshot);

            try (BufferedWriter writer = Files.newBufferedWriter(
                    this.blockchainFile,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                for (Block block : snapshot) {
                    writer.write(jsonMapper.writeValueAsString(block));
                    writer.newLine();
                }
            } catch (IOException e) {
                BrokerLogger.warn(LogType.ALERTA, "BLOCKCHAIN_LOCAL_NAO_GRAVADA", "arquivo", this.blockchainFile.toString(), "erro", e.getMessage());
                return;
            }

            if (ledger != null) {
                for (Block block : snapshot) {
                    if (block.getHash() != null && persistedBlockHashes.add(block.getHash())) {
                        ledger.recordBlockchainCopy(block, origin, valid);
                    }
                }
                ledger.recordChainSnapshot(this.blockchainFile, snapshot.size(), valid);
            }
        }
    }

    private void attachBlockchainReplicationListeners() {
        this.sharedBlockchain.addEntryListener((EntryAddedListener<String, Block>) event -> syncLocalChainFromDistributed(), true);
        this.sharedBlockchain.addEntryListener((EntryUpdatedListener<String, Block>) event -> syncLocalChainFromDistributed(), true);
        this.sharedBlockchain.addEntryListener((EntryRemovedListener<String, Block>) event -> syncLocalChainFromDistributed(), true);
    }

    private void initializeBlockchainState() {
        FencedLock lock = hz.getCPSubsystem().getLock(BLOCKCHAIN_LOCK_NAME);
        lock.lock();
        try {
            loadPersistentBlockchainIntoDistributedIfUsefulLocked();
            ensureGenesisExistsLocked();
            syncLocalChainFromDistributed();
            alignGlobalIndexWithLocalChainLocked();
            ensureKnownSectorIssuesLocked();
            syncLocalChainFromDistributed();
            refreshDerivedCachesFromLocalChain();
        } finally {
            lock.unlock();
        }
    }

    private void ensureGenesisExistsLocked() {
        boolean genesisExists = this.sharedBlockchain.values().stream().anyMatch(b -> b != null && b.getIndex() == 0);
        if (!genesisExists) {
            Block genesis = Blockchain.createGenesisBlock();
            this.sharedBlockchain.put(genesis.getHash(), genesis);
        }
    }

    private void alignGlobalIndexWithLocalChainLocked() {
        IAtomicLong globalIndex = hz.getCPSubsystem().getAtomicLong(BLOCKCHAIN_INDEX_NAME);
        int latestLocal = localChain.getLastIndex();
        if (globalIndex.get() < latestLocal) {
            globalIndex.set(latestLocal);
        }
    }

    private void ensureKnownSectorIssuesLocked() {
        for (Map.Entry<String, Integer> entry : INITIAL_SECTOR_BALANCES.entrySet()) {
            ensureSectorAccountLocked(entry.getKey(), entry.getValue());
        }
    }

    private void ensureSectorAccountLocked(String sector, int initialBalance) {
        syncLocalChainFromDistributed();
        if (!Blockchain.hasInitialIssueForSector(localChain.getChain(), sector)) {
            Transaction issue = Transaction.issue(sector, initialBalance, "EMISSAO_INICIAL_SETOR");
            appendBlockLocallyAndPublishLocked(issue);
            BrokerLogger.event(LogType.BLOCKCHAIN, "SALDO_INICIAL_EMITIDO", "setor", sector, "tokens", String.valueOf(initialBalance));
        }
    }

    private boolean isKnownSector(String sector) {
        return sector != null && INITIAL_SECTOR_BALANCES.containsKey(sector);
    }

    private int initialBalanceForSector(String sector) {
        Integer balance = INITIAL_SECTOR_BALANCES.get(sector);
        if (balance == null) {
            throw new IllegalArgumentException("Setor operacional desconhecido: " + sector);
        }
        return balance;
    }

    private void syncLocalChainFromDistributed() {
        List<Block> ordered = Blockchain.orderedCopy(this.sharedBlockchain != null ? this.sharedBlockchain.values() : List.of());
        if (ordered.isEmpty()) {
            ordered = List.of(Blockchain.createGenesisBlock());
        }
        this.localChain.replaceChain(ordered);
        persistLocalChainSnapshot("SYNC_DISTRIBUIDO");
    }

    private void refreshDerivedCachesFromLocalChain() {
        syncLocalChainFromDistributed();
        Map<String, Integer> balances = new LinkedHashMap<>(Blockchain.calculateAllSectorBalances(localChain.getChain()));
        this.sectorBalances.clear();
        if (!balances.isEmpty()) this.sectorBalances.putAll(balances);

        this.usedTokenIds.clear();
        for (Block block : localChain.getChain()) {
            Transaction tx = block.getTransaction();
            if (tx != null && tx.getType() == TransactionType.PAYMENT && tx.getTokenId() != null) {
                this.usedTokenIds.add(tx.getTokenId());
            }
        }
    }

    private Block appendBlockGlobally(Transaction tx) {
        FencedLock lock = hz.getCPSubsystem().getLock(BLOCKCHAIN_LOCK_NAME);
        lock.lock();
        try {
            loadPersistentBlockchainIntoDistributedIfUsefulLocked();
            ensureGenesisExistsLocked();
            syncLocalChainFromDistributed();
            if (!localChain.isChainValid()) {
                throw new IllegalStateException("Cadeia local inválida antes da inclusão de novo bloco");
            }
            Block block = appendBlockLocallyAndPublishLocked(tx);
            refreshDerivedCachesFromLocalChain();
            return block;
        } finally {
            lock.unlock();
        }
    }

    /** Deve ser chamado com o FencedLock já adquirido. */
    private Block appendBlockLocallyAndPublishLocked(Transaction tx) {
        alignGlobalIndexWithLocalChainLocked();
        IAtomicLong globalIndex = hz.getCPSubsystem().getAtomicLong(BLOCKCHAIN_INDEX_NAME);
        long newIndex = globalIndex.incrementAndGet();

        String previousHash = localChain.getLastHash();
        Block block = new Block(Math.toIntExact(newIndex), tx, previousHash);
        block.setHash(Blockchain.calculateHash(block));

        // Produção local primeiro; Hazelcast apenas replica para os demais brokers.
        this.localChain.forceAddBlock(block);
        if (!localChain.isChainValid()) {
            throw new IllegalStateException("Bloco produzido localmente gerou cadeia inválida");
        }
        persistLocalChainSnapshot("PRODUCAO_LOCAL");

        this.sharedBlockchain.put(block.getHash(), block);
        return block;
    }

    private DebitResult debitToken(Message message) {
        if (message == null || message.getSectorId() == null || message.getSectorId().isBlank()
                || message.getTokenId() == null || message.getTokenId().isBlank()
                || message.getServiceType() == null) {
            return DebitResult.rejected(DebitFailureReason.INVALID_REQUEST);
        }

        if (!isKnownSector(message.getSectorId())) {
            BrokerLogger.warn(LogType.ALERTA, "SETOR_DESCONHECIDO", "setor", message.getSectorId(), "nó", message.getNodeId());
            appendRejectedPaymentAuditIfPossible(message, DebitFailureReason.INVALID_REQUEST.description + ": setor desconhecido");
            return DebitResult.rejected(DebitFailureReason.INVALID_REQUEST);
        }

        if (!SectorAuthenticator.verify(message)) {
            BrokerLogger.warn(LogType.ALERTA, "ASSINATURA_INVÁLIDA", "setor", message.getSectorId(), "nó", message.getNodeId());
            appendRejectedPaymentAuditIfPossible(message, DebitFailureReason.INVALID_REQUEST.description);
            return DebitResult.rejected(DebitFailureReason.INVALID_REQUEST);
        }

        FencedLock lock = hz.getCPSubsystem().getLock(BLOCKCHAIN_LOCK_NAME);
        lock.lock();
        try {
            ensureGenesisExistsLocked();
            ensureSectorAccountLocked(message.getSectorId(), initialBalanceForSector(message.getSectorId()));
            syncLocalChainFromDistributed();

            if (!localChain.isChainValid()) {
                BrokerLogger.warn(LogType.ALERTA, "DÉBITO_REJEITADO", "motivo", "cadeia inválida");
                return DebitResult.rejected(DebitFailureReason.CHAIN_INVALID);
            }

            if (Blockchain.hasConfirmedPaymentWithToken(localChain.getChain(), message.getTokenId())) {
                BrokerLogger.warn(LogType.ALERTA, "DUPLO_GASTO", "setor", message.getSectorId(), "token", message.getTokenId());
                Transaction rejected = Transaction.rejectedPayment(message.getTokenId(), message.getSectorId(), message.getServiceType().name(), message.getNodeId(), DebitFailureReason.DOUBLE_SPEND.description);
                appendBlockLocallyAndPublishLocked(rejected);
                refreshDerivedCachesFromLocalChain();
                if (ledger != null) ledger.recordDoubleSpend(message.getSectorId(), message.getTokenId());
                return DebitResult.rejected(DebitFailureReason.DOUBLE_SPEND);
            }

            int oldBalance = localChain.getBalance(message.getSectorId());
            if (oldBalance <= 0) {
                BrokerLogger.warn(LogType.ALERTA, "SALDO_INSUFICIENTE", "setor", message.getSectorId(), "saldo", String.valueOf(oldBalance));
                Transaction rejected = Transaction.rejectedPayment(message.getTokenId(), message.getSectorId(), message.getServiceType().name(), message.getNodeId(), DebitFailureReason.INSUFFICIENT_BALANCE.description);
                appendBlockLocallyAndPublishLocked(rejected);
                refreshDerivedCachesFromLocalChain();
                if (ledger != null) ledger.recordInsufficientBalance(message.getSectorId(), oldBalance);
                return DebitResult.rejected(DebitFailureReason.INSUFFICIENT_BALANCE);
            }

            Transaction tx = Transaction.payment(
                    message.getTokenId(),
                    message.getSectorId(),
                    message.getServiceType().name(),
                    message.getNodeId(),
                    message.getRequestTimestamp(),
                    message.getRequestSignature()
            );
            tx.setMissionId(message.getMissionId());
            tx.setRouteId(message.getRouteId());
            Block block = appendBlockLocallyAndPublishLocked(tx);
            int newBalance = localChain.getBalance(message.getSectorId());
            refreshDerivedCachesFromLocalChain();

            BrokerLogger.event(LogType.BLOCKCHAIN, "DÉBITO_APROVADO", "bloco", "#" + block.getIndex(), "setor", message.getSectorId(), "saldo", oldBalance + " -> " + newBalance, "serviço", message.getServiceType().name());
            if (ledger != null) ledger.recordBlock(block.getIndex(), message.getSectorId(), message.getServiceType().name(), message.getTokenId(), newBalance, block.getHash(), block.getPreviousHash());
            return DebitResult.approved(newBalance, block);
        } catch (RuntimeException e) {
            BrokerLogger.warn(LogType.ALERTA, "FALHA_CONSENSO_DÉBITO", "erro", e.getMessage());
            if (ledger != null) ledger.recordCasFailure(message.getSectorId());
            return DebitResult.rejected(DebitFailureReason.CONSENSUS_FAILURE);
        } finally {
            lock.unlock();
        }
    }

    private void appendRejectedPaymentAuditIfPossible(Message message, String reason) {
        try {
            Transaction rejected = Transaction.rejectedPayment(
                    message != null ? message.getTokenId() : null,
                    message != null ? message.getSectorId() : null,
                    message != null && message.getServiceType() != null ? message.getServiceType().name() : "UNKNOWN_SERVICE",
                    message != null ? message.getNodeId() : null,
                    reason
            );
            appendBlockGlobally(rejected);
        } catch (RuntimeException ignored) {
            // Se a cadeia estiver comprometida, o próprio erro de validação aparecerá no monitor.
        }
    }

    private void notifyPaymentRejected(Message original, DebitResult debitResult) {
        Message reject = new Message();
        reject.setCommandType(CommandType.PAYMENT_REJECTED);
        reject.setSectorId(original.getSectorId());
        reject.setNodeId("BROKER-" + brokerId.substring(0, 8));
        reject.setTargetNodeId(original.getNodeId());
        reject.setServiceType(original.getServiceType());
        reject.setTokenId(original.getTokenId());
        reject.setMissionReport(debitResult.reasonText());
        reject.setData(0);
        this.clusterWideBus.publish(reject);
        BrokerLogger.warn(LogType.ALERTA, "REJEIÇÃO_NOTIFICADA", "destino", original.getNodeId(), "motivo", debitResult.reasonText());
    }

    private boolean validateCreditCommand(Message message) {
        if (message == null || message.getCommandType() != CommandType.ADD_TOKENS) return false;
        if (message.getSectorId() == null || message.getSectorId().isBlank()) return false;
        if (!isKnownSector(message.getSectorId())) {
            BrokerLogger.warn(LogType.ALERTA, "RECARGA_REJEITADA", "motivo", "setor desconhecido", "setor", message.getSectorId(), "origem", message.getNodeId());
            return false;
        }
        if (message.getData() <= 0) return false;
        if (!SectorAuthenticator.verifyAdmin(message)) {
            BrokerLogger.warn(LogType.ALERTA, "RECARGA_REJEITADA", "motivo", "assinatura administrativa inválida", "setor", message.getSectorId(), "origem", message.getNodeId());
            return false;
        }
        return true;
    }

    private void rejectInvalidCreditCommand(Message message) {
        try {
            Transaction rejected = Transaction.rejectedPayment(
                    message != null ? message.getTokenId() : null,
                    message != null ? message.getSectorId() : null,
                    "ADD_TOKENS",
                    message != null ? message.getNodeId() : null,
                    "Recarga administrativa rejeitada: assinatura inválida ou parâmetros inválidos"
            );
            appendBlockGlobally(rejected);
        } catch (RuntimeException ignored) {
            // Não interrompe o broker se o ledger já estiver indisponível/adulterado.
        }
    }

    private boolean validateMissionCompletion(String droneId, DroneInterface oldDrone, Message message) {
        if (message == null) return false;
        if (droneId == null || droneId.isBlank()) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "motivo", "drone ausente");
            return false;
        }
        if (!SectorAuthenticator.verify(message)) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "assinatura inválida");
            return false;
        }
        if (oldDrone == null) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "drone não registrado");
            return false;
        }
        if (!oldDrone.isInUse()) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "drone não estava em missão");
            return false;
        }
        if (!sameRequired(oldDrone.getPendingMissionId(), message.getMissionId())) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "missionId divergente", "esperado", oldDrone.getPendingMissionId(), "recebido", message.getMissionId());
            return false;
        }
        if (!sameRequired(oldDrone.getPendingRouteId(), message.getRouteId())) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "routeId divergente", "esperado", oldDrone.getPendingRouteId(), "recebido", message.getRouteId());
            return false;
        }
        if (oldDrone.getPendingTaskSectorId() != null && !oldDrone.getPendingTaskSectorId().equals(message.getSectorId())) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "setor divergente", "esperado", oldDrone.getPendingTaskSectorId(), "recebido", message.getSectorId());
            return false;
        }
        if (oldDrone.getPendingServiceType() != null && oldDrone.getPendingServiceType() != message.getServiceType()) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "serviço divergente", "esperado", oldDrone.getPendingServiceType().name(), "recebido", message.getServiceType() != null ? message.getServiceType().name() : "NULO");
            return false;
        }
        if (message.getMissionReport() == null || message.getMissionReport().isBlank()) {
            BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "laudo ausente");
            return false;
        }
        return true;
    }

    private boolean sameRequired(String expected, String received) {
        return expected != null && !expected.isBlank() && expected.equals(received);
    }

    private void creditTokens(Message message) {
        String sector = message != null ? message.getSectorId() : null;
        int amount = message != null ? message.getData() : 0;
        if (sector == null || sector.isBlank() || !isKnownSector(sector)) {
            BrokerLogger.warn(LogType.ALERTA, "RECARGA_IGNORADA", "motivo", "setor inválido ou desconhecido", "setor", String.valueOf(sector));
            return;
        }
        if (amount <= 0) {
            BrokerLogger.warn(LogType.ALERTA, "RECARGA_IGNORADA", "motivo", "quantidade inválida", "quantidade", String.valueOf(amount));
            return;
        }

        FencedLock lock = hz.getCPSubsystem().getLock(BLOCKCHAIN_LOCK_NAME);
        lock.lock();
        try {
            ensureGenesisExistsLocked();
            ensureSectorAccountLocked(sector, initialBalanceForSector(sector));
            syncLocalChainFromDistributed();
            if (message.getTokenId() != null && Blockchain.hasTransactionWithToken(localChain.getChain(), message.getTokenId())) {
                BrokerLogger.warn(LogType.ALERTA, "RECARGA_DUPLICADA_IGNORADA", "setor", sector, "token", message.getTokenId());
                return;
            }
            int old = localChain.getBalance(sector);
            Transaction tx = Transaction.credit(sector, amount, message.getNodeId(), message.getRequestSignature());
            tx.setTokenId(message.getTokenId());
            tx.setRequestTimestamp(message.getRequestTimestamp());
            Block block = appendBlockLocallyAndPublishLocked(tx);
            int updated = localChain.getBalance(sector);
            refreshDerivedCachesFromLocalChain();
            if (ledger != null) ledger.recordCredit(sector, amount, updated);
            BrokerLogger.event(LogType.BLOCKCHAIN, "RECARGA_REGISTRADA", "bloco", "#" + block.getIndex(), "setor", sector, "saldo", old + " -> " + updated, "tokens", String.valueOf(amount), "admin", message.getNodeId());
        } finally {
            lock.unlock();
        }
    }

    private void recordMissionReport(String droneId, DroneInterface oldDrone, Message message) {
        String sector = oldDrone != null && oldDrone.getPendingTaskSectorId() != null
                ? oldDrone.getPendingTaskSectorId()
                : message.getSectorId();
        String service = oldDrone != null && oldDrone.getPendingServiceType() != null
                ? oldDrone.getPendingServiceType().name()
                : (message.getServiceType() != null ? message.getServiceType().name() : "MISSION_LOG");
        String report = message.getMissionReport();
        String route = message.getRouteId() != null && !message.getRouteId().isBlank()
                ? message.getRouteId()
                : "ROTA-" + sector + "-" + droneId;
        String missionId = message.getMissionId() != null && !message.getMissionId().isBlank()
                ? message.getMissionId()
                : "MIS-" + UUID.randomUUID();

        Transaction logTx = Transaction.missionLog(missionId, sector, service, droneId, route, report);
        if (oldDrone != null && oldDrone.getPendingPaymentTokenId() != null && !oldDrone.getPendingPaymentTokenId().isBlank()) {
            logTx.setTokenId(oldDrone.getPendingPaymentTokenId());
        }
        Block logBlock = appendBlockGlobally(logTx);

        BrokerLogger.event(LogType.BLOCKCHAIN, "LAUDO_REGISTRADO", "bloco", "#" + logBlock.getIndex(), "drone", droneId, "setor", sector, "rota", route, "laudo", report);
        if (ledger != null) {
            ledger.recordMissionReport(logBlock.getIndex(), droneId, sector, service, route, report, logBlock.getHash(), logBlock.getPreviousHash());
        }
    }

    private void cleanUpOrphanedData(String crashedBrokerId) {
        for (Map.Entry<String, ClientData> entry : this.sharedClients.entrySet()) {
            ClientData client = entry.getValue();
            if (crashedBrokerId.equals(client.getConnectedBrokerId())) {
                this.sharedClients.remove(entry.getKey());
                String key = client.getSectorId() + ":" + client.getTopic();
                this.sharedTopics.remove(key, client.getNodeId());
            }
        }

        Predicate<String, DroneInterface> condition = Predicates.equal("currentBrokerId", crashedBrokerId);
        for (Map.Entry<String, DroneInterface> entry : this.sharedDrones.entrySet(condition)) {
            DroneInterface crashed = entry.getValue();
            if (this.sharedDrones.remove(entry.getKey(), crashed)) {
                if (crashed.isInUse() && crashed.getPendingTaskSectorId() != null && crashed.getPendingServiceType() != null) {
                    requeuePaidMission(crashed, "BROKER_DA_MISSAO_REMOVIDO");
                }
            }
        }
    }

    @Override
    public void runServer(boolean isRunning) {
        BrokerLogger.event(LogType.SISTEMA, "AGUARDANDO_CONEXÕES", "broker", brokerId.substring(0, 8), "porta", String.valueOf(serverSocket.getLocalPort()));
        try {
            while (isRunning) {
                Socket socket = this.serverSocket.accept();
                MiddlewareHandlerInterface handler = new MiddlewareHandlerBuilder().withSocket(socket).withBroker(this).build();
                new Thread(handler).start();
            }
        } catch (IOException e) {
            BrokerLogger.err(LogType.SISTEMA, "Servidor interrompido: " + e.getMessage());
        }
    }

    public void addConnection(TopicType topic, String nodeId, MiddlewareHandlerInterface handler) {
        if (nodeId == null || nodeId.isBlank()) {
            BrokerLogger.warn(LogType.CONEXAO, "CONEXAO_IGNORADA", "motivo", "nodeId ausente");
            return;
        }
        this.clientsPerTopic.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(handler);
        Set<MiddlewareHandlerInterface> nodeConnections = this.connectionsById.computeIfAbsent(nodeId, k -> ConcurrentHashMap.newKeySet());
        boolean firstConnectionForNode = nodeConnections.isEmpty();
        nodeConnections.add(handler);
        this.clientsById.put(nodeId, handler);

        if (nodeId != null && nodeId.startsWith("DRN") && firstConnectionForNode) {
            DroneInterface drone = new DroneData(nodeId, false, this.brokerId);
            DroneInterface previous = this.sharedDrones.putIfAbsent(nodeId, drone);
            if (previous == null) {
                checkDistributedQueue();
            } else if (!previous.isInUse()) {
                DroneInterface updated = clearMissionState(previous);
                updated.setCurrentBrokerId(this.brokerId);
                this.sharedDrones.replace(nodeId, previous, updated);
                checkDistributedQueue();
            } else {
                BrokerLogger.event(LogType.DRONE, "DRONE_RECONEXAO_PRESERVADA", "drone", nodeId, "estado", "missão em andamento não sobrescrita");
            }
        }
    }

    public void removeConnection(TopicType topic, String nodeId, MiddlewareHandlerInterface handler) {
        if (topic != null && this.clientsPerTopic.containsKey(topic)) this.clientsPerTopic.get(topic).remove(handler);
        if (nodeId == null) return;

        Set<MiddlewareHandlerInterface> remaining = this.connectionsById.get(nodeId);
        if (remaining != null) {
            remaining.remove(handler);
            if (!remaining.isEmpty()) {
                MiddlewareHandlerInterface replacement = remaining.iterator().next();
                if (this.clientsById.get(nodeId) == handler) {
                    this.clientsById.put(nodeId, replacement);
                }
                return;
            }
            this.connectionsById.remove(nodeId);
        }

        this.clientsById.remove(nodeId);
        this.sharedClients.remove(nodeId);
        if (nodeId.startsWith("DRN")) {
            DroneInterface fallen = this.sharedDrones.get(nodeId);
            if (fallen != null && fallen.isInUse() && fallen.getPendingTaskSectorId() != null && fallen.getPendingServiceType() != null) {
                this.sharedDrones.remove(nodeId);
                requeuePaidMission(fallen, "DRONE_DESCONECTADO");
            } else {
                this.sharedDrones.remove(nodeId);
            }
        }
    }

    private void checkDistributedQueue() {
        Message pending = this.filaRequisicoes.poll();
        if (pending != null) {
            processMessage(pending);
        }
    }

    public void processMessage(Message message) {
        if (message == null) return;

        if (message.getCommandType() == CommandType.ADD_TOKENS) {
            if (validateCreditCommand(message)) {
                creditTokens(message);
            } else {
                rejectInvalidCreditCommand(message);
            }
            return;
        }

        if ("BROKER_SIGNAL_COMPLETION".equals(message.getTargetNodeId())) {
            String droneId = message.getNodeId();
            DroneInterface oldDrone = this.sharedDrones.get(droneId);
            if (!validateMissionCompletion(droneId, oldDrone, message)) {
                return;
            }

            DroneInterface freed = clearMissionState(oldDrone);
            freed.setInUse(false);
            boolean replaced = this.sharedDrones.replace(droneId, oldDrone, freed);

            if (replaced) {
                recordMissionReport(droneId, oldDrone, message);
                checkDistributedQueue();
            } else {
                BrokerLogger.warn(LogType.ALERTA, "CONCLUSAO_REJEITADA", "drone", droneId, "motivo", "estado do drone mudou durante a confirmação");
            }
            return;
        }

        if (message.getCommandType() == CommandType.REASSIGN_SECTOR || message.getCommandType() == CommandType.SET_DATA_TYPES) {
            this.clusterWideBus.publish(message);
            return;
        }

        if (message.getCommandType() == CommandType.REASSIGN_MISSION) {
            processPaidMissionReassignment(message);
            return;
        }

        if (message.getServiceType() == null) return;

        if (isEscortRequest(message)) {
            assignDroneForMission(message, false);
        } else {
            this.clusterWideBus.publish(message);
        }
    }

    private boolean isEscortRequest(Message message) {
        return message != null
                && message.getData() == 1
                && (message.getServiceType() == ServicesTypes.INTRUSION_DETECTION
                    || message.getServiceType() == ServicesTypes.SEARCH_AND_RESCUE);
    }

    private void processPaidMissionReassignment(Message message) {
        if (!isEscortRequest(message)) {
            BrokerLogger.warn(LogType.ALERTA, "REATRIBUICAO_IGNORADA", "motivo", "mensagem não é requisição de escolta", "mensagem", String.valueOf(message));
            return;
        }
        if (message.getNodeId() == null || !message.getNodeId().startsWith("BROKER-REASSIGN-")) {
            BrokerLogger.warn(LogType.ALERTA, "REATRIBUICAO_IGNORADA", "motivo", "origem não é broker interno", "origem", message.getNodeId());
            return;
        }
        if (!SectorAuthenticator.verify(message)) {
            BrokerLogger.warn(LogType.ALERTA, "REATRIBUICAO_IGNORADA", "motivo", "assinatura inválida", "origem", message.getNodeId());
            return;
        }
        if (!hasConfirmedPaymentForReassignment(message)) {
            BrokerLogger.warn(LogType.ALERTA, "REATRIBUICAO_IGNORADA", "motivo", "pagamento original não encontrado ou missão já concluída no ledger", "token", message.getTokenId(), "missão", message.getMissionId());
            return;
        }
        assignDroneForMission(message, true);
    }

    private void assignDroneForMission(Message message, boolean paymentAlreadyConfirmed) {
        Predicate<String, DroneInterface> condition = Predicates.equal("inUse", false);
        boolean droneFound = false;

        for (Map.Entry<String, DroneInterface> entry : this.sharedDrones.entrySet(condition)) {
            DroneInterface oldDrone = entry.getValue();
            ServicesTypes droneService = droneServiceForRequest(message.getServiceType());
            ensureMissionMetadata(message, oldDrone.getDroneId());

            DroneInterface next = new DroneData(oldDrone);
            next.setInUse(true);
            next.setPendingTaskSectorId(message.getSectorId());
            next.setPendingServiceType(droneService);
            next.setPendingMissionId(message.getMissionId());
            next.setPendingRouteId(message.getRouteId());
            next.setPendingPaymentTokenId(message.getTokenId());
            next.setPendingOriginalRequesterId(message.getNodeId());
            next.setActivatedAt(System.currentTimeMillis());

            if (this.sharedDrones.replace(entry.getKey(), oldDrone, next)) {
                if (!paymentAlreadyConfirmed) {
                    DebitResult debitResult = debitToken(message);
                    if (!debitResult.approved) {
                        DroneInterface freed = clearMissionState(oldDrone);
                        freed.setInUse(false);
                        this.sharedDrones.replace(entry.getKey(), next, freed);
                        notifyPaymentRejected(message, debitResult);
                        checkDistributedQueue();
                        droneFound = true;
                        break;
                    }
                }

                Message cmd = new Message();
                cmd.setTargetNodeId(next.getDroneId());
                cmd.setCommandType(CommandType.ACTIVATE);
                cmd.setSectorId(message.getSectorId());
                cmd.setServiceType(droneService);
                cmd.setMissionId(message.getMissionId());
                cmd.setRouteId(message.getRouteId());
                cmd.setTokenId("ACT-" + message.getMissionId());
                cmd.setNodeId("BROKER-" + brokerId.substring(0, 8));
                SectorAuthenticator.sign(cmd);
                this.clusterWideBus.publish(cmd);

                BrokerLogger.event(LogType.DRONE,
                        paymentAlreadyConfirmed ? "MISSAO_REATRIBUIDA_SEM_DEBITO" : "MISSAO_DESPACHADA",
                        "drone", next.getDroneId(),
                        "setor", message.getSectorId(),
                        "missão", message.getMissionId(),
                        "rota", message.getRouteId(),
                        "serviço", droneService.name(),
                        "tokenPagamento", message.getTokenId());
                droneFound = true;
                break;
            }
        }

        if (!droneFound) {
            this.filaRequisicoes.offer(message);
            BrokerLogger.event(LogType.DRONE, "REQUISIÇÃO_ENFILEIRADA", "setor", message.getSectorId(), "serviço", message.getServiceType().name(), "motivo", "nenhum drone livre", "semNovoDébito", String.valueOf(paymentAlreadyConfirmed));
        }
    }

    private void ensureMissionMetadata(Message message, String droneId) {
        if (message.getMissionId() == null || message.getMissionId().isBlank()) {
            message.setMissionId("MIS-" + System.currentTimeMillis() + "-" + droneId + "-" + UUID.randomUUID().toString().substring(0, 8));
        }
        if (message.getRouteId() == null || message.getRouteId().isBlank()) {
            message.setRouteId("ROTA-" + message.getSectorId() + "-" + droneId);
        }
    }

    private ServicesTypes droneServiceForRequest(ServicesTypes requestService) {
        return requestService == ServicesTypes.INTRUSION_DETECTION
                ? ServicesTypes.VISUAL_RECONNAISSANCE
                : ServicesTypes.SEARCH_AND_RESCUE;
    }

    private ServicesTypes requestServiceForDroneService(ServicesTypes droneService) {
        return droneService == ServicesTypes.VISUAL_RECONNAISSANCE
                ? ServicesTypes.INTRUSION_DETECTION
                : ServicesTypes.SEARCH_AND_RESCUE;
    }

    private DroneInterface clearMissionState(DroneInterface source) {
        DroneInterface cleaned = new DroneData(source);
        cleaned.setPendingTaskSectorId(null);
        cleaned.setPendingServiceType(null);
        cleaned.setPendingMissionId(null);
        cleaned.setPendingRouteId(null);
        cleaned.setPendingPaymentTokenId(null);
        cleaned.setPendingOriginalRequesterId(null);
        cleaned.setActivatedAt(null);
        return cleaned;
    }

    private void requeuePaidMission(DroneInterface failedDrone, String reason) {
        Message reassign = buildReassignmentMessage(failedDrone, reason);
        if (reassign == null) {
            BrokerLogger.warn(LogType.ALERTA, "REATRIBUICAO_ABORTADA", "motivo", "metadados da missão paga ausentes", "drone", failedDrone != null ? failedDrone.getDroneId() : "N/A");
            return;
        }
        this.filaRequisicoes.offer(reassign);
        BrokerLogger.warn(LogType.DRONE, "REATRIBUICAO_ENFILEIRADA", "droneOriginal", failedDrone.getDroneId(), "missão", reassign.getMissionId(), "motivo", reason, "novoDébito", "false");
        checkDistributedQueue();
    }

    private Message buildReassignmentMessage(DroneInterface failedDrone, String reason) {
        if (failedDrone == null
                || failedDrone.getPendingTaskSectorId() == null
                || failedDrone.getPendingServiceType() == null
                || failedDrone.getPendingMissionId() == null
                || failedDrone.getPendingPaymentTokenId() == null) {
            return null;
        }
        Message reassign = new Message();
        reassign.setCommandType(CommandType.REASSIGN_MISSION);
        reassign.setSectorId(failedDrone.getPendingTaskSectorId());
        reassign.setServiceType(requestServiceForDroneService(failedDrone.getPendingServiceType()));
        reassign.setData(1);
        reassign.setNodeId("BROKER-REASSIGN-" + brokerId.substring(0, 8));
        reassign.setTokenId(failedDrone.getPendingPaymentTokenId());
        reassign.setMissionId(failedDrone.getPendingMissionId());
        reassign.setRouteId(failedDrone.getPendingRouteId());
        reassign.setMissionReport("REATRIBUICAO_SEM_NOVO_DEBITO:" + reason);
        SectorAuthenticator.sign(reassign);
        return reassign;
    }

    private boolean hasConfirmedPaymentForReassignment(Message message) {
        if (message == null || message.getTokenId() == null || message.getTokenId().isBlank()) return false;
        FencedLock lock = hz.getCPSubsystem().getLock(BLOCKCHAIN_LOCK_NAME);
        lock.lock();
        try {
            syncLocalChainFromDistributed();
            return localChain.isChainValid()
                    && Blockchain.hasConfirmedPaymentForMission(localChain.getChain(), message.getTokenId(), message.getMissionId(), message.getSectorId(), message.getRouteId())
                    && !Blockchain.hasMissionLogForMission(localChain.getChain(), message.getMissionId());
        } finally {
            lock.unlock();
        }
    }

    private void deliverLocal(Message message) {
        if (message == null) return;

        if (message.getTargetNodeId() != null && this.clientsById.containsKey(message.getTargetNodeId())) {
            this.clientsById.get(message.getTargetNodeId()).sendMessage(message);
            return;
        }

        if (message.getServiceType() == null) return;
        String key = (message.getSectorId() != null ? message.getSectorId() : "DEFAULT") + ":" + message.getServiceType().getDefaultTopic();
        Collection<String> ids = this.sharedTopics.get(key);
        if (ids == null || ids.isEmpty()) return;
        for (String id : ids) {
            if (message.getTargetNodeId() != null && !message.getTargetNodeId().equals(id)) continue;
            MiddlewareHandlerInterface local = this.clientsById.get(id);
            if (local != null) local.sendMessage(message);
        }
    }

    public IMap<String, ClientData>     getSharedClients()    { return sharedClients; }
    public MultiMap<String, String>     getSharedTopics()     { return sharedTopics; }
    public IMap<String, DroneInterface> getSharedDrones()     { return sharedDrones; }
    public ITopic<Message>              getClusterWideBus()   { return clusterWideBus; }
    public IMap<String, Block>          getSharedBlockchain() { return sharedBlockchain; }
    public IMap<String, Integer>        getSectorBalances()   { return sectorBalances; }
    public ISet<String>                 getUsedTokenIds()     { return usedTokenIds; }
    public Blockchain                   getLocalChain()       { return localChain; }
    public String                       getBrokerId()         { return brokerId; }
}
