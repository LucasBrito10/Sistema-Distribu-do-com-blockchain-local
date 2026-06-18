package com.ormuz.client.app;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.ormuz.client.core.Client;
import com.ormuz.shared.blockchain.Blockchain;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.enums.TransactionType;
import com.ormuz.shared.interfaces.DroneInterface;
import com.ormuz.shared.security.SectorAuthenticator;
import com.ormuz.shared.types.Block;
import com.ormuz.shared.types.ClientData;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.types.Transaction;

/**
 * Cenário automatizado de demonstração para o barema do Problema 3.
 *
 * <p>Este aplicativo foi criado para ser executado dentro da rede Docker do
 * ORMUZ. Ele provisiona drones pelas fábricas já existentes, recarrega tokens,
 * dispara requisições concorrentes de escolta, força um duplo gasto com o mesmo
 * token e, ao final, imprime uma auditoria da blockchain: validade da cadeia,
 * saldos derivados do ledger, pagamentos, rejeições e laudos de missão.</p>
 *
 * <p>Execução via Docker Compose:</p>
 * <pre>
 * docker compose -f docker-compose.dev.yaml --profile admin run --rm teste-barema
 * </pre>
 */
public class AppBaremaAuditTest extends Client {

    private static final String DEFAULT_SECTORS = "SETOR_NORTE,SETOR_SUL,SETOR_LESTE,SETOR_OESTE";

    private final AtomicInteger rejectedNotifications = new AtomicInteger(0);

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg == null || msg.getCommandType() != CommandType.PAYMENT_REJECTED) return;

        rejectedNotifications.incrementAndGet();
        System.out.printf("%n[TESTE][RETORNO_BROKER] Pagamento rejeitado | setor=%s | token=%s | motivo=%s%n",
                msg.getSectorId(), msg.getTokenId(), msg.getMissionReport());
    }

    public static void main(String[] args) throws Exception {
        String clusterIps = args.length > 0 ? args[0] : env("BROKER_IP", "127.0.0.1");
        int port = args.length > 1 ? Integer.parseInt(args[1]) : intEnv("BROKER_PORT", 8080);
        String defaultSector = args.length > 2 ? args[2] : env("SECTOR", "SETOR_NORTE");

        List<String> sectors = parseCsv(env("TEST_SECTORS", DEFAULT_SECTORS));
        int dronesPerSector = intEnv("TEST_DRONES_PER_SECTOR", 3);
        int normalRequests = intEnv("TEST_REQUESTS", 8);
        int creditAmount = intEnv("TEST_CREDIT_AMOUNT", 5);
        int waitSeconds = intEnv("TEST_WAIT_SECONDS", 55);
        boolean runDoubleSpend = boolEnv("TEST_DOUBLE_SPEND", true);
        boolean runBalanceSpend = boolEnv("TEST_BALANCE_DOUBLE_SPEND", true);

        AppBaremaAuditTest tester = new AppBaremaAuditTest();
        tester.setId("BAREMA-TEST-" + System.currentTimeMillis());
        tester.setSectorId(defaultSector);

        printHeader("CENÁRIO AUTOMATIZADO DO BAREMA - ORMUZ");
        System.out.println("Cluster/Hazelcast/Brokers: " + clusterIps);
        System.out.println("Porta TCP dos brokers: " + port);
        System.out.println("Setores testados: " + String.join(", ", sectors));
        System.out.println("Drones solicitados por setor: " + dronesPerSector);
        System.out.println("Requisições normais: " + normalRequests);
        System.out.println("Duplo gasto com mesmo token: " + (runDoubleSpend ? "habilitado" : "desabilitado"));
        System.out.println("Concorrência sobre o mesmo saldo: " + (runBalanceSpend ? "habilitada" : "desabilitada"));

        // O mesmo cliente abre publishers para os serviços críticos e subscribers
        // para receber notificações de rejeição de pagamento.
        tester.startPublisher(clusterIps, port, ServicesTypes.INTRUSION_DETECTION);
        tester.startPublisher(clusterIps, port, ServicesTypes.SEARCH_AND_RESCUE);
        tester.startPublisher(clusterIps, port, ServicesTypes.VISUAL_RECONNAISSANCE);
        tester.startSubscribe(clusterIps, port, ServicesTypes.INTRUSION_DETECTION);
        tester.startSubscribe(clusterIps, port, ServicesTypes.SEARCH_AND_RESCUE);
        Thread.sleep(2_000);

        HazelcastInstance hzClient = createHazelcastClient(clusterIps);
        IMap<String, Block> chainMap = hzClient.getMap("ormuz-shared-blockchain");
        IMap<String, DroneInterface> dronesMap = hzClient.getMap("DroneMap");
        IMap<String, ClientData> clientsMap = hzClient.getMap("global-clients-map");

        try {
            int initialBlocks = chainMap.size();
            int initialDrones = dronesMap.size();

            printHeader("1) ESTADO INICIAL DA REDE");
            printNetworkSummary(chainMap, dronesMap, clientsMap);

            printHeader("2) PROVISIONANDO DRONES VIA FÁBRICAS DO CLUSTER");
            waitForDroneFactories(clientsMap, sectors, 30_000);
            for (String sector : sectors) {
                for (int i = 1; i <= dronesPerSector; i++) {
                    tester.sendProvisionDroneCommand(sector, i);
                }
            }
            waitForAtLeast("drones registrados", () -> dronesMap.size(), initialDrones + (sectors.size() * dronesPerSector), 20_000);
            printDroneSummary(dronesMap);

            printHeader("3) RECARREGANDO TOKENS PARA EVITAR FALSO NEGATIVO POR SALDO");
            for (String sector : sectors) {
                tester.sendCreditCommand(sector, creditAmount);
            }
            waitForAtLeast("blocos após recarga", () -> chainMap.size(), initialBlocks + sectors.size(), 20_000);
            printBalances(chainMap);

            printHeader("4) DISPARANDO REQUISIÇÕES DE ESCOLTA");
            int paymentsBefore = countTx(chainMap, TransactionType.PAYMENT);
            int missionLogsBefore = countTx(chainMap, TransactionType.MISSION_LOG);
            int rejectedBefore = countTx(chainMap, TransactionType.PAYMENT_REJECTED);

            for (int i = 1; i <= normalRequests; i++) {
                String sector = sectors.get((i - 1) % sectors.size());
                ServicesTypes service = (i % 2 == 0)
                        ? ServicesTypes.SEARCH_AND_RESCUE
                        : ServicesTypes.INTRUSION_DETECTION;
                String token = "REQ-BAREMA-" + sector + "-" + i + "-" + UUID.randomUUID();
                tester.sendEscortRequest(sector, service, token, "SENSOR-BAREMA-" + i);
                Thread.sleep(250);
            }

            if (runDoubleSpend) {
                printHeader("5) TESTE DE DUPLO GASTO COM O MESMO TOKEN");
                String duplicateToken = "DOUBLE-SPEND-BAREMA-" + UUID.randomUUID();
                CountDownLatch startGate = new CountDownLatch(1);
                ExecutorService executor = Executors.newFixedThreadPool(2);
                for (int i = 1; i <= 2; i++) {
                    executor.submit(() -> {
                        try {
                            startGate.await();
                            // Usa o próprio id do cliente de teste para que a notificação PAYMENT_REJECTED
                            // volte ao AppBaremaAuditTest e conte corretamente no relatório final.
                            tester.sendEscortRequest(defaultSector, ServicesTypes.INTRUSION_DETECTION,
                                    duplicateToken, tester.getId());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
                System.out.println("Enviando duas requisições simultâneas com token repetido: " + duplicateToken);
                startGate.countDown();
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }

            int balanceStressRequests = 0;
            int balanceStressApprovedExpected = 0;
            int balanceStressRejectedExpected = 0;
            if (runBalanceSpend) {
                printHeader("5B) TESTE CONCORRENTE USANDO O MESMO SALDO DO SETOR");
                String stressSector = env("TEST_BALANCE_STRESS_SECTOR", "SETOR_OESTE");
                int availableBalance = Blockchain.calculateAllSectorBalances(chainMap.values()).getOrDefault(stressSector, 0);
                int freeDrones = (int) dronesMap.values().stream().filter(d -> !d.isInUse()).count();
                balanceStressRequests = Math.max(2, Math.min(availableBalance + 1, freeDrones));

                if (availableBalance <= 0 || balanceStressRequests < 2) {
                    System.out.printf("[TESTE] Teste de saldo pulado: setor=%s saldo=%d dronesLivres=%d%n",
                            stressSector, availableBalance, freeDrones);
                    balanceStressRequests = 0;
                } else {
                    balanceStressApprovedExpected = Math.min(balanceStressRequests, availableBalance);
                    balanceStressRejectedExpected = Math.max(0, balanceStressRequests - availableBalance);
                    CountDownLatch startGate = new CountDownLatch(1);
                    ExecutorService executor = Executors.newFixedThreadPool(balanceStressRequests);
                    for (int i = 1; i <= balanceStressRequests; i++) {
                        final int n = i;
                        executor.submit(() -> {
                            try {
                                startGate.await();
                                tester.sendEscortRequest(stressSector, ServicesTypes.SEARCH_AND_RESCUE,
                                        "BALANCE-RACE-" + stressSector + "-" + n + "-" + UUID.randomUUID(), tester.getId());
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
                    }
                    System.out.printf("Enviando %d requisições simultâneas com tokens diferentes para o saldo atual do setor %s (%d tokens).%n",
                            balanceStressRequests, stressSector, availableBalance);
                    startGate.countDown();
                    executor.shutdown();
                    executor.awaitTermination(10, TimeUnit.SECONDS);
                }
            }

            int expectedNewPayments = normalRequests + (runDoubleSpend ? 1 : 0) + balanceStressApprovedExpected;
            int expectedNewRejections = (runDoubleSpend ? 1 : 0) + balanceStressRejectedExpected;

            System.out.println("\n[TESTE] Aguardando confirmações de pagamento no ledger...");
            waitForAtLeast("pagamentos confirmados", () -> countTx(chainMap, TransactionType.PAYMENT),
                    paymentsBefore + expectedNewPayments, 30_000);

            if (expectedNewRejections > 0) {
                waitForAtLeast("pagamentos rejeitados", () -> countTx(chainMap, TransactionType.PAYMENT_REJECTED),
                        rejectedBefore + expectedNewRejections, 30_000);
            }

            System.out.println("\n[TESTE] Aguardando conclusão dos drones para registrar laudos de missão...");
            System.out.println("Tempo de espera configurado: " + waitSeconds + "s");
            Thread.sleep(waitSeconds * 1_000L);

            printHeader("6) AUDITORIA FINAL DA BLOCKCHAIN");
            int paymentsAfter = countTx(chainMap, TransactionType.PAYMENT);
            int missionLogsAfter = countTx(chainMap, TransactionType.MISSION_LOG);
            int rejectedAfter = countTx(chainMap, TransactionType.PAYMENT_REJECTED);

            System.out.println("Blocos totais: " + chainMap.size());
            System.out.println("Pagamentos confirmados no teste: " + (paymentsAfter - paymentsBefore));
            System.out.println("Pagamentos rejeitados no teste: " + (rejectedAfter - rejectedBefore));
            System.out.println("Laudos registrados no teste: " + (missionLogsAfter - missionLogsBefore));
            System.out.println("Notificações de rejeição recebidas pelo cliente: " + tester.rejectedNotifications.get());
            System.out.println("Validade da cadeia distribuída: "
                    + (Blockchain.isChainValid(new ArrayList<>(chainMap.values())) ? "✅ VÁLIDA" : "❌ INVÁLIDA/ADULTERADA"));

            printBalances(chainMap);
            printTransactionCounters(chainMap);
            printMissionLogs(chainMap, 30);
            printLastBlocks(chainMap, 20);

            printHeader("7) COMO CONFERIR NO MONITOR");
            System.out.println("Abra o monitor e use as opções:");
            System.out.println("  6 -> Exibir Blockchain Global");
            System.out.println("  7 -> Validar Cadeia Distribuída");
            System.out.println("  8 -> Exibir Saldos dos Setores");
            System.out.println("  9 -> Exibir Tokens Utilizados");

        } finally {
            hzClient.shutdown();
        }

        System.out.println("\n[TESTE] Cenário finalizado.");
        System.exit(0);
    }

    private void sendProvisionDroneCommand(String sector, int sequence) {
        Message cmd = new Message();
        cmd.setCommandType(CommandType.ACTIVATE);
        cmd.setTargetNodeId("FACTORY-DRONES-" + sector);
        cmd.setSectorId(sector);
        cmd.setServiceType(ServicesTypes.VISUAL_RECONNAISSANCE);
        cmd.setNodeId("BAREMA-PROVISIONER");
        cmd.setTokenId("PROVISION-DRONE-" + sector + "-" + sequence + "-" + UUID.randomUUID());
        SectorAuthenticator.sign(cmd);
        sendCommandMessage(cmd);
        System.out.printf("[TESTE] Comando de provisionamento enviado: setor=%s alvo=%s%n",
                sector, cmd.getTargetNodeId());
    }

    private void sendCreditCommand(String sector, int amount) {
        Message cmd = new Message();
        cmd.setCommandType(CommandType.ADD_TOKENS);
        cmd.setSectorId(sector);
        cmd.setData(amount);
        cmd.setNodeId("BAREMA-CREDIT-TEST");
        cmd.setTokenId("CREDIT-BAREMA-" + sector + "-" + UUID.randomUUID());
        SectorAuthenticator.signAdmin(cmd);
        sendCommandMessage(cmd);
        System.out.printf("[TESTE] Recarga solicitada: setor=%s +%d tokens%n", sector, amount);
    }

    private void sendEscortRequest(String sector, ServicesTypes service, String token, String sensorId) {
        Message request = new Message();
        request.setData(1);
        request.setNodeId(sensorId);
        request.setSectorId(sector);
        request.setServiceType(service);
        request.setConnectionType("PUBLISHER");
        request.setTokenId(token);
        request.setMissionId("MIS-" + sensorId + "-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8));
        request.setRouteId("ROTA-" + sector + "-" + sensorId);
        SectorAuthenticator.sign(request);
        sendCommandMessage(request);
        System.out.printf("[TESTE] Requisição enviada | setor=%s | serviço=%s | token=%s | sensor=%s%n",
                sector, service, token, sensorId);
    }

    private static HazelcastInstance createHazelcastClient(String clusterIps) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("ormuz-cluster");
        for (String host : parseCsv(clusterIps)) {
            clientConfig.getNetworkConfig().addAddress(host + ":5701");
        }
        return HazelcastClient.newHazelcastClient(clientConfig);
    }


    private static void waitForDroneFactories(IMap<String, ClientData> clientsMap, List<String> sectors, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<String> missing;
        do {
            missing = sectors.stream()
                    .map(sector -> "FACTORY-DRONES-" + sector)
                    .filter(factoryId -> !clientsMap.containsKey(factoryId))
                    .collect(Collectors.toList());
            if (missing.isEmpty()) {
                System.out.println("[TESTE] OK: fábricas de drones registradas para todos os setores.");
                return;
            }
            Thread.sleep(500);
        } while (System.currentTimeMillis() < deadline);

        System.out.println("[TESTE] AVISO: nem todas as fábricas de drones apareceram no mapa distribuído.");
        System.out.println("Fábricas ausentes: " + String.join(", ", missing));
        System.out.println("O teste continuará, mas o provisionamento desses setores pode não gerar drones.");
    }

    private static void printNetworkSummary(IMap<String, Block> chainMap,
                                            IMap<String, DroneInterface> dronesMap,
                                            IMap<String, ClientData> clientsMap) {
        System.out.println("Clientes/equipamentos registrados: " + clientsMap.size());
        System.out.println("Drones registrados: " + dronesMap.size());
        System.out.println("Blocos na blockchain: " + chainMap.size());
        System.out.println("Cadeia válida: " + (Blockchain.isChainValid(new ArrayList<>(chainMap.values())) ? "SIM" : "NÃO"));
        printBalances(chainMap);
    }

    private static void printDroneSummary(IMap<String, DroneInterface> dronesMap) {
        System.out.println("\n--- DRONES REGISTRADOS ---");
        if (dronesMap.isEmpty()) {
            System.out.println("Nenhum drone registrado. Verifique se os containers drones-* estão em execução.");
            return;
        }
        dronesMap.values().stream()
                .sorted((a, b) -> a.getDroneId().compareToIgnoreCase(b.getDroneId()))
                .forEach(d -> System.out.printf("  • %-45s status=%s setorMissao=%s serviçoPendente=%s%n",
                        d.getDroneId(), d.isInUse() ? "EM_MISSAO" : "LIVRE",
                        d.getPendingTaskSectorId(), d.getPendingServiceType()));
    }

    private static void printBalances(IMap<String, Block> chainMap) {
        System.out.println("\n--- SALDOS DERIVADOS DO HISTÓRICO DA BLOCKCHAIN ---");
        Map<String, Integer> balances = Blockchain.calculateAllSectorBalances(chainMap.values());
        if (balances.isEmpty()) {
            System.out.println("Nenhum saldo encontrado.");
            return;
        }
        balances.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.printf("  • %-15s %d tokens%n", e.getKey(), e.getValue()));
    }

    private static void printTransactionCounters(IMap<String, Block> chainMap) {
        Map<TransactionType, Integer> counters = new EnumMap<>(TransactionType.class);
        for (Block block : Blockchain.orderedCopy(chainMap.values())) {
            Transaction tx = block.getTransaction();
            if (tx != null && tx.getType() != null) {
                counters.merge(tx.getType(), 1, Integer::sum);
            }
        }

        System.out.println("\n--- CONTAGEM POR TIPO DE TRANSAÇÃO ---");
        for (TransactionType type : TransactionType.values()) {
            System.out.printf("  • %-18s %d%n", type, counters.getOrDefault(type, 0));
        }
    }

    private static void printMissionLogs(IMap<String, Block> chainMap, int limit) {
        List<Block> logs = Blockchain.orderedCopy(chainMap.values()).stream()
                .filter(b -> b.getTransaction() != null)
                .filter(b -> b.getTransaction().getType() == TransactionType.MISSION_LOG)
                .collect(Collectors.toList());

        System.out.println("\n--- LAUDOS DE MISSÃO REGISTRADOS NO LEDGER ---");
        if (logs.isEmpty()) {
            System.out.println("Nenhum laudo encontrado. Aguarde os drones concluírem ou aumente TEST_WAIT_SECONDS.");
            return;
        }

        logs.stream().skip(Math.max(0, logs.size() - limit)).forEach(block -> {
            Transaction tx = block.getTransaction();
            System.out.printf("  #%d | missão=%s | setor=%s | drone=%s | rota=%s | serviço=%s | laudo=%s%n",
                    block.getIndex(), tx.getMissionId(), tx.getFromSector(), tx.getDroneId(),
                    tx.getRouteId(), tx.getServiceType(), tx.getMissionReport());
        });
    }

    private static void printLastBlocks(IMap<String, Block> chainMap, int limit) {
        List<Block> blocks = new ArrayList<>(Blockchain.orderedCopy(chainMap.values()));
        int from = Math.max(0, blocks.size() - limit);

        System.out.println("\n--- ÚLTIMOS BLOCOS DA BLOCKCHAIN ---");
        for (Block block : blocks.subList(from, blocks.size())) {
            Transaction tx = block.getTransaction();
            String hash = abbreviate(block.getHash());
            String prev = abbreviate(block.getPreviousHash());
            if (tx == null) {
                System.out.printf("  #%d | hash=%s | prev=%s | transação=null%n", block.getIndex(), hash, prev);
            } else {
                System.out.printf("  #%d | %-16s | setor=%s -> %s | qtd=%d | serviço=%s | token=%s | hash=%s | prev=%s%n",
                        block.getIndex(), tx.getType(), tx.getFromSector(), tx.getToSector(), tx.getAmount(),
                        tx.getServiceType(), abbreviate(tx.getTokenId()), hash, prev);
            }
        }
    }

    private static int countTx(IMap<String, Block> chainMap, TransactionType type) {
        int count = 0;
        for (Block block : chainMap.values()) {
            Transaction tx = block.getTransaction();
            if (tx != null && tx.getType() == type) count++;
        }
        return count;
    }

    private static void waitForAtLeast(String label, IntSupplierChecked supplier, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int current;
        do {
            current = supplier.getAsInt();
            if (current >= expected) {
                System.out.printf("[TESTE] OK: %s = %d (esperado >= %d)%n", label, current, expected);
                return;
            }
            Thread.sleep(500);
        } while (System.currentTimeMillis() < deadline);

        current = supplier.getAsInt();
        System.out.printf("[TESTE] AVISO: timeout aguardando %s. Atual=%d, esperado >= %d%n",
                label, current, expected);
    }

    @FunctionalInterface
    private interface IntSupplierChecked {
        int getAsInt();
    }

    private static List<String> parseCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        List<String> values = new ArrayList<>();
        for (String part : csv.split(",")) {
            String value = part.trim();
            if (!value.isBlank() && !values.contains(value)) values.add(value);
        }
        return values;
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) return fallback;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static boolean boolEnv(String key, boolean fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) return fallback;
        return switch (value.trim().toLowerCase()) {
            case "1", "true", "yes", "sim", "s" -> true;
            case "0", "false", "no", "nao", "não", "n" -> false;
            default -> fallback;
        };
    }

    private static String abbreviate(String value) {
        if (value == null) return "null";
        return value.length() <= 16 ? value : value.substring(0, 16) + "...";
    }

    private static void printHeader(String title) {
        System.out.println("\n============================================================");
        System.out.println(" " + title);
        System.out.println("============================================================");
    }
}
