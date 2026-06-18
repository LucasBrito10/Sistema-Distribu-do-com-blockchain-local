package com.ormuz.shared.blockchain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ormuz.shared.enums.TransactionType;
import com.ormuz.shared.types.Block;
import com.ormuz.shared.types.Transaction;

/**
 * Representação local e utilitários da blockchain ORMUZ.
 *
 * <p>Cada broker mantém sua própria instância local desta classe. Hazelcast é
 * usado apenas para replicar os blocos e serializar o consenso, não como fonte
 * central de saldo. O saldo de cada setor é sempre recalculável pelo histórico
 * de transações da cadeia.</p>
 */
public class Blockchain {

    /** Hash anterior usado exclusivamente pelo bloco gênesis. */
    public static final String GENESIS_HASH =
            "0000000000000000000000000000000000000000000000000000000000000000";

    private final List<Block> chain = new ArrayList<>();

    public Blockchain() {
        chain.add(createGenesisBlock());
    }

    public static Block createGenesisBlock() {
        Transaction genesisTx = Transaction.genesis();
        Block genesis = new Block(0, genesisTx, GENESIS_HASH);
        genesis.setTimestamp(0L);
        genesis.setHash(calculateHash(genesis));
        return genesis;
    }

    public synchronized void forceAddBlock(Block block) {
        if (block != null) chain.add(block);
    }

    /** Substitui a cópia local inteira por uma versão ordenada recebida da rede. */
    public synchronized void replaceChain(List<Block> blocks) {
        chain.clear();
        List<Block> ordered = orderedCopy(blocks);
        if (ordered.isEmpty()) {
            chain.add(createGenesisBlock());
        } else {
            chain.addAll(ordered);
        }
    }

    public synchronized String getLastHash() {
        return chain.isEmpty() ? GENESIS_HASH : chain.get(chain.size() - 1).getHash();
    }

    public synchronized int getLastIndex() {
        return chain.isEmpty() ? -1 : chain.get(chain.size() - 1).getIndex();
    }

    public synchronized int size() {
        return chain.size();
    }

    public synchronized List<Block> getChain() {
        return Collections.unmodifiableList(new ArrayList<>(chain));
    }

    public synchronized int getBalance(String sector) {
        return calculateSectorBalance(chain, sector);
    }

    /**
     * Calcula o SHA-256 canônico do bloco. Todos os campos da transação que
     * alteram significado são incluídos, inclusive laudo, drone e rota.
     */
    public static String calculateHash(Block block) {
        StringBuilder input = new StringBuilder();
        appendField(input, "index", block.getIndex());
        appendField(input, "timestamp", block.getTimestamp());
        appendField(input, "previousHash", block.getPreviousHash());

        Transaction tx = block.getTransaction();
        if (tx == null) {
            appendField(input, "tx", "null");
        } else {
            appendField(input, "tx.transactionId", tx.getTransactionId());
            appendField(input, "tx.type", tx.getType());
            appendField(input, "tx.tokenId", tx.getTokenId());
            appendField(input, "tx.fromSector", tx.getFromSector());
            appendField(input, "tx.toSector", tx.getToSector());
            appendField(input, "tx.amount", tx.getAmount());
            appendField(input, "tx.serviceType", tx.getServiceType());
            appendField(input, "tx.timestamp", tx.getTimestamp());
            appendField(input, "tx.requestNodeId", tx.getRequestNodeId());
            appendField(input, "tx.requestTimestamp", tx.getRequestTimestamp());
            appendField(input, "tx.requestSignature", tx.getRequestSignature());
            appendField(input, "tx.missionId", tx.getMissionId());
            appendField(input, "tx.droneId", tx.getDroneId());
            appendField(input, "tx.routeId", tx.getRouteId());
            appendField(input, "tx.missionReport", tx.getMissionReport());
        }

        return sha256Hex(input.toString());
    }

    private static void appendField(StringBuilder sb, String name, Object value) {
        String text = value == null ? "<null>" : value.toString();
        sb.append(name).append('=').append(text.length()).append(':').append(text).append('\n');
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hashBytes.length * 2);
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) sb.append('0');
                sb.append(hex);
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }

    public synchronized boolean isChainValid() {
        return isChainValid(chain);
    }

    public static boolean isChainValid(List<Block> chainToValidate) {
        if (chainToValidate == null || chainToValidate.isEmpty()) return false;

        List<Block> ordered = orderedCopy(chainToValidate);

        Block genesis = ordered.get(0);
        if (genesis.getIndex() != 0) return false;
        if (!GENESIS_HASH.equals(genesis.getPreviousHash())) return false;
        if (genesis.getHash() == null || !genesis.getHash().equals(calculateHash(genesis))) return false;

        for (int i = 1; i < ordered.size(); i++) {
            Block cur = ordered.get(i);
            Block prev = ordered.get(i - 1);

            if (cur.getIndex() != prev.getIndex() + 1) return false;
            if (cur.getHash() == null || !cur.getHash().equals(calculateHash(cur))) return false;
            if (cur.getPreviousHash() == null || !cur.getPreviousHash().equals(prev.getHash())) return false;
        }
        return true;
    }

    public static List<Block> orderedCopy(Iterable<Block> blocks) {
        List<Block> ordered = new ArrayList<>();
        if (blocks != null) {
            for (Block block : blocks) {
                if (block != null) ordered.add(block);
            }
        }
        ordered.sort(Comparator.comparingInt(Block::getIndex).thenComparingLong(Block::getTimestamp));
        return ordered;
    }

    /** Calcula saldo exclusivamente a partir de ISSUE/CREDIT/PAYMENT confirmados. */
    public static int calculateSectorBalance(Iterable<Block> blocks, String sector) {
        if (sector == null || sector.isBlank()) return 0;
        int balance = 0;
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx == null || tx.getType() == null) continue;
            TransactionType type = tx.getType();
            switch (type) {
                case ISSUE, CREDIT -> {
                    if (sector.equals(tx.getToSector())) balance += Math.max(tx.getAmount(), 0);
                }
                case PAYMENT -> {
                    if (sector.equals(tx.getFromSector())) balance -= Math.max(tx.getAmount(), 0);
                }
                default -> { }
            }
        }
        return balance;
    }

    public static Map<String, Integer> calculateAllSectorBalances(Iterable<Block> blocks) {
        Map<String, Integer> balances = new LinkedHashMap<>();
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx == null || tx.getType() == null) continue;
            switch (tx.getType()) {
                case ISSUE, CREDIT -> {
                    if (tx.getToSector() != null) {
                        balances.merge(tx.getToSector(), Math.max(tx.getAmount(), 0), Integer::sum);
                    }
                }
                case PAYMENT -> {
                    if (tx.getFromSector() != null) {
                        balances.merge(tx.getFromSector(), -Math.max(tx.getAmount(), 0), Integer::sum);
                    }
                }
                default -> { }
            }
        }
        return balances;
    }

    public static boolean hasInitialIssueForSector(Iterable<Block> blocks, String sector) {
        if (sector == null) return false;
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx != null && tx.getType() == TransactionType.ISSUE && sector.equals(tx.getToSector())) {
                return true;
            }
        }
        return false;
    }

    /** Verifica duplo gasto pelo ledger, não por variável local. */
    public static boolean hasConfirmedPaymentWithToken(Iterable<Block> blocks, String tokenId) {
        if (tokenId == null || tokenId.isBlank()) return false;
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx != null && tx.getType() == TransactionType.PAYMENT && tokenId.equals(tx.getTokenId())) {
                return true;
            }
        }
        return false;
    }

    /** Verifica se qualquer transação já materializou um token/idempotency key. */
    public static boolean hasTransactionWithToken(Iterable<Block> blocks, String tokenId) {
        if (tokenId == null || tokenId.isBlank()) return false;
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx != null && tokenId.equals(tx.getTokenId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Confirma que o pagamento usado para reatribuição corresponde à mesma missão.
     * Isso evita que uma reatribuição reaproveite um pagamento antigo de outra missão.
     */
    public static boolean hasConfirmedPaymentForMission(Iterable<Block> blocks, String tokenId, String missionId, String sector, String routeId) {
        if (tokenId == null || tokenId.isBlank()) return false;
        if (missionId == null || missionId.isBlank()) return false;
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx == null || tx.getType() != TransactionType.PAYMENT) continue;
            if (!tokenId.equals(tx.getTokenId())) continue;
            if (!missionId.equals(tx.getMissionId())) continue;
            if (sector != null && !sector.isBlank() && !sector.equals(tx.getFromSector())) continue;
            if (routeId != null && !routeId.isBlank() && !routeId.equals(tx.getRouteId())) continue;
            return true;
        }
        return false;
    }

    public static boolean hasMissionLogForMission(Iterable<Block> blocks, String missionId) {
        if (missionId == null || missionId.isBlank()) return false;
        for (Block block : orderedCopy(blocks)) {
            Transaction tx = block.getTransaction();
            if (tx != null && tx.getType() == TransactionType.MISSION_LOG && missionId.equals(tx.getMissionId())) {
                return true;
            }
        }
        return false;
    }
}
