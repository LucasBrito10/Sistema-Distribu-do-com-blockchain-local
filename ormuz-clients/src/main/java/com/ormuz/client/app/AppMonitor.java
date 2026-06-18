package com.ormuz.client.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
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
import com.ormuz.shared.types.Block;
import com.ormuz.shared.types.ClientData;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.types.Transaction;
import com.ormuz.shared.util.ConsoleLog;

public class AppMonitor extends Client {

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg.getData() == -1) return;
        if (msg.getServiceType() == null) return;

        String interpretacao = interpretarMensagem(msg);
        ConsoleLog.info(
                "MONITOR",
                "DADO_RECEBIDO",
                "serviço", msg.getServiceType().name(),
                "setor", msg.getSectorId(),
                "origem", msg.getNodeId(),
                "valor", String.valueOf(msg.getData()),
                "interpretação", interpretacao
        );
    }

    public static void main(String[] args) throws InterruptedException {
        String clusterIps = args.length > 0 ? args[0] : "127.0.0.1";
        int    porta      = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String setor      = args.length > 2 ? args[2] : "SETOR_NORTE";

        AppMonitor monitor = new AppMonitor();
        monitor.setId("MONITOR-CENTRAL");
        monitor.setSectorId(setor);

        for (ServicesTypes s : ServicesTypes.values()) {
            monitor.startSubscribe(clusterIps, porta, s);
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("ormuz-cluster");
        for (String h : clusterIps.split(",")) {
            if (h != null && !h.isBlank()) {
                clientConfig.getNetworkConfig().addAddress(h.trim() + ":5701");
            }
        }
        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<String, DroneInterface> dronesMap  = hzClient.getMap("DroneMap");
        IMap<String, ClientData>     clientsMap = hzClient.getMap("global-clients-map");
        IMap<String, Block>          chainMap   = hzClient.getMap("ormuz-shared-blockchain");

        ConsoleLog.section("TERMINAL DE MONITORAMENTO ORMUZ");
        System.out.println(ConsoleLog.row("Brokers do cluster", clusterIps));
        System.out.println(ConsoleLog.row("Porta", String.valueOf(porta)));
        System.out.println(ConsoleLog.row("Setor do monitor", setor));
        System.out.println(ConsoleLog.row("ID", monitor.getId()));

        try (Scanner scanner = new Scanner(System.in)) {
            int opcao = 0;
            while (opcao != 11) {
                exibirMenu();
                try {
                    opcao = Integer.parseInt(scanner.nextLine().trim());
                } catch (NumberFormatException e) {
                    opcao = 0;
                }

                switch (opcao) {
                    case 1 -> listarFrotaDrones(dronesMap);
                    case 2 -> listarEquipamentos(clientsMap);
                    case 3 -> listarDataTypesPorSetor(clientsMap);
                    case 4 -> configurarDataTypesSensor(scanner, monitor);
                    case 5 -> reassociarRecurso(scanner);
                    case 6 -> listarBlockchain(chainMap);
                    case 7 -> validarCadeia(chainMap);
                    case 8 -> listarSaldos(chainMap);
                    case 9 -> listarTokensUsados(chainMap);
                    case 10 -> { for (int i = 0; i < 50; i++) System.out.println(); }
                    case 11 -> {
                        ConsoleLog.info("MONITOR", "ENCERRANDO", "status", "cliente Hazelcast será desligado");
                        hzClient.shutdown();
                        System.exit(0);
                    }
                    default -> ConsoleLog.warn("MONITOR", "OPÇÃO_INVÁLIDA", "opção", String.valueOf(opcao));
                }
            }
        }
    }

    private static String interpretarMensagem(Message msg) {
        return switch (msg.getServiceType()) {
            case HYDROGRAPHIC_PROFILING -> "Profundidade " + msg.getData() + "m; status estável se dentro do limite.";
            case PRESSURE_SENSING -> "Pressão barométrica " + msg.getData() + " kPa.";
            case INTRUSION_DETECTION -> msg.getData() == 1
                    ? "ALERTA CRÍTICO: embarcação hostil/não identificada."
                    : "Área limpa.";
            case TRAFFIC_MONITORING -> "Tráfego marítimo registrado; unidades=" + msg.getData() + ".";
            case LONG_RANGE_SURVEILLANCE -> "Varredura de longo alcance; alertas=" + msg.getData() + ".";
            case VISUAL_RECONNAISSANCE -> msg.getData() == 0
                    ? "Drone retornou; nenhuma anomalia visual."
                    : "Imagens confirmam alvo/anomalia.";
            default -> "Valor lido=" + msg.getData() + ".";
        };
    }

    private static void exibirMenu() {
        System.out.println();
        System.out.println("════════════════════════════════════════════════════════════");
        System.out.println("  MENU DO MONITOR ORMUZ");
        System.out.println("════════════════════════════════════════════════════════════");
        System.out.println("  1  Status da frota de drones");
        System.out.println("  2  Equipamentos conectados");
        System.out.println("  3  Tipos de dado ativos por setor");
        System.out.println("  4  Configurar tipos de dado de sensores");
        System.out.println("  5  Reassociar recurso a outro setor");
        System.out.println("  6  Exibir blockchain global / auditoria");
        System.out.println("  7  Validar cadeia distribuída");
        System.out.println("  8  Exibir saldos dos setores");
        System.out.println("  9  Exibir tokens utilizados");
        System.out.println(" 10  Limpar console");
        System.out.println(" 11  Sair");
        System.out.print("Escolha uma opção: ");
    }

    private static void listarFrotaDrones(IMap<String, DroneInterface> dronesMap) {
        ConsoleLog.subsection("STATUS DA FROTA DE DRONES");
        if (dronesMap.isEmpty()) {
            System.out.println("Nenhum drone registrado.");
            return;
        }

        System.out.printf("%-46s %-13s %-28s %-18s %-14s%n", "DRONE", "STATUS", "SERVIÇO PENDENTE", "SETOR MISSÃO", "BROKER");
        System.out.println("─".repeat(126));
        dronesMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    DroneInterface d = entry.getValue();
                    String status = d.isInUse() ? "EM MISSÃO" : "LIVRE";
                    String servico = d.getPendingServiceType() != null ? d.getPendingServiceType().name() : "-";
                    String setor = d.getPendingTaskSectorId() != null ? d.getPendingTaskSectorId() : "-";
                    System.out.printf("%-46s %-13s %-28s %-18s %-14s%n",
                            ConsoleLog.shortText(d.getDroneId(), 45),
                            status,
                            servico,
                            setor,
                            ConsoleLog.shortText(d.getCurrentBrokerId(), 13));
                });
    }

    private static void listarEquipamentos(IMap<String, ClientData> clientsMap) {
        ConsoleLog.subsection("EQUIPAMENTOS ONLINE");
        if (clientsMap.isEmpty()) {
            System.out.println("Nenhum cliente registrado.");
            return;
        }

        Map<String, List<ClientData>> porSetor = clientsMap.values().stream()
                .collect(Collectors.groupingBy(c -> c.getSectorId() != null ? c.getSectorId() : "SEM_SETOR"));

        porSetor.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            System.out.println("\n[" + entry.getKey() + "]");
            System.out.printf("%-46s %-14s %-28s%n", "NÓ", "TIPO", "TÓPICO/SERVIÇO");
            System.out.println("─".repeat(92));
            entry.getValue().stream()
                    .sorted((a, b) -> ConsoleLog.safe(a.getNodeId()).compareTo(ConsoleLog.safe(b.getNodeId())))
                    .forEach(c -> System.out.printf("%-46s %-14s %-28s%n",
                            ConsoleLog.shortText(c.getNodeId(), 45),
                            ConsoleLog.safe(c.getConnectionType()),
                            c.getTopic() != null ? c.getTopic().name() : "N/A"));
        });
    }

    private static void listarDataTypesPorSetor(IMap<String, ClientData> clientsMap) {
        ConsoleLog.subsection("TIPOS DE DADO ATIVOS POR SETOR");
        if (clientsMap.isEmpty()) {
            System.out.println("Nenhum cliente registrado.");
            return;
        }

        Map<String, List<ClientData>> porSetor = clientsMap.values().stream()
                .filter(c -> c.getSectorId() != null)
                .collect(Collectors.groupingBy(ClientData::getSectorId));

        porSetor.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            System.out.println("\n[" + entry.getKey() + "]");
            Map<String, List<ClientData>> porTipo = entry.getValue().stream()
                    .collect(Collectors.groupingBy(c -> c.getTopic() != null ? c.getTopic().name() : "N/A"));
            porTipo.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(tipo -> {
                String ids = tipo.getValue().stream()
                        .map(ClientData::getNodeId)
                        .sorted()
                        .collect(Collectors.joining(", "));
                System.out.printf("  %-34s -> %s%n", tipo.getKey(), ids);
            });
        });
    }

    private static void configurarDataTypesSensor(Scanner scanner, AppMonitor monitor) {
        ConsoleLog.subsection("CONFIGURAR TIPOS DE DADO DOS SENSORES");
        System.out.print("Setor alvo (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();

        System.out.println("\nTipos de dado disponíveis:");
        ServicesTypes[] todos = ServicesTypes.values();
        for (int i = 0; i < todos.length; i++) {
            System.out.printf("  %2d. %-35s recurso=%s%n", i + 1, todos[i].name(), todos[i].getRelatedResource());
        }
        System.out.println("\nDigite os números a habilitar, separados por vírgula. Em branco = TODOS.");
        System.out.print("Seleção: ");
        String selecao = scanner.nextLine().trim();

        List<ServicesTypes> selecionados;
        if (selecao.isBlank()) {
            selecionados = Arrays.asList(todos);
        } else {
            selecionados = new ArrayList<>();
            for (String parte : selecao.split(",")) {
                try {
                    int idx = Integer.parseInt(parte.trim()) - 1;
                    if (idx >= 0 && idx < todos.length) selecionados.add(todos[idx]);
                } catch (NumberFormatException ignored) {}
            }
        }

        if (selecionados.isEmpty()) {
            ConsoleLog.warn("MONITOR", "CONFIGURAÇÃO_IGNORADA", "motivo", "nenhum tipo válido selecionado");
            return;
        }

        Message cmd = new Message();
        cmd.setCommandType(CommandType.SET_DATA_TYPES);
        cmd.setTargetNodeId("FACTORY-SENSORES-" + setor);
        cmd.setSectorId(setor);
        cmd.setDataTypes(selecionados);

        monitor.sendCommandMessage(cmd);
        ConsoleLog.info(
                "MONITOR",
                "SET_DATA_TYPES_ENVIADO",
                "destino", "FACTORY-SENSORES-" + setor,
                "tipos", selecionados.stream().map(ServicesTypes::name).collect(Collectors.joining(","))
        );
    }

    private static void reassociarRecurso(Scanner scanner) {
        ConsoleLog.subsection("REASSOCIAR RECURSO A OUTRO SETOR");
        System.out.print("ID do recurso: ");
        scanner.nextLine().trim();
        System.out.print("Novo setor (ex: SETOR_SUL): ");
        scanner.nextLine().trim().toUpperCase();
        System.out.print("Broker do novo setor (ex: broker-sul): ");
        scanner.nextLine().trim();
        ConsoleLog.warn(
                "MONITOR",
                "REASSOCIAÇÃO_NÃO_ENVIADA",
                "orientação", "use o painel do AppBroker opção 2 para enviar o comando com acesso seguro ao barramento"
        );
    }

    private static void listarBlockchain(IMap<String, Block> chainMap) {
        ConsoleLog.subsection("BLOCKCHAIN DISTRIBUÍDA / AUDITORIA");
        List<Block> blocks = Blockchain.orderedCopy(chainMap.values());
        if (blocks.isEmpty()) {
            System.out.println("Nenhum bloco encontrado.");
            return;
        }

        System.out.println(ConsoleLog.row("Total de blocos", String.valueOf(blocks.size())));
        System.out.println(ConsoleLog.row("Cadeia válida", Blockchain.isChainValid(blocks) ? "SIM" : "NÃO"));

        for (Block block : blocks) {
            Transaction tx = block.getTransaction();
            String tipo = tx != null && tx.getType() != null ? tx.getType().name() : "SEM_TX";
            System.out.println();
            System.out.printf("BLOCO %-7s | %-17s | %s%n", blockRef(block.getIndex()), tipo, ConsoleLog.formatEpochMillis(block.getTimestamp()));
            System.out.println("  hash      : " + ConsoleLog.shortText(block.getHash(), 24));
            System.out.println("  anterior  : " + ConsoleLog.shortText(block.getPreviousHash(), 24));

            if (tx == null) {
                System.out.println("  transação : N/A");
                continue;
            }

            System.out.println("  tx id     : " + ConsoleLog.shortText(tx.getTransactionId(), 42));
            System.out.println("  fluxo     : " + ConsoleLog.safe(tx.getFromSector()) + " -> " + ConsoleLog.safe(tx.getToSector()));
            System.out.println("  serviço   : " + ConsoleLog.safe(tx.getServiceType()));
            System.out.println("  qtd/token : " + tx.getAmount() + " / " + ConsoleLog.shortText(tx.getTokenId(), 42));

            if (tx.getRequestNodeId() != null) {
                System.out.println("  requisit. : " + tx.getRequestNodeId());
                System.out.println("  assinatura: " + ConsoleLog.shortText(tx.getRequestSignature(), 24));
            }
            if (tx.getMissionReport() != null) {
                System.out.println("  missão    : " + ConsoleLog.safe(tx.getMissionId()));
                System.out.println("  drone     : " + ConsoleLog.safe(tx.getDroneId()));
                System.out.println("  rota      : " + ConsoleLog.safe(tx.getRouteId()));
                System.out.println("  laudo     : " + tx.getMissionReport());
            }
        }
    }

    private static void validarCadeia(IMap<String, Block> chainMap) {
        ConsoleLog.subsection("AUDITORIA DE INTEGRIDADE");
        List<Block> blocks = Blockchain.orderedCopy(chainMap.values());
        boolean valid = Blockchain.isChainValid(blocks);
        System.out.println(ConsoleLog.row("Blocos analisados", String.valueOf(blocks.size())));
        System.out.println(ConsoleLog.row("Resultado", valid ? "VÁLIDA" : "ADULTERADA/INVÁLIDA"));
        if (valid) {
            ConsoleLog.info("MONITOR", "CADEIA_VÁLIDA", "status", "nenhuma quebra de hash ou linearidade detectada");
        } else {
            ConsoleLog.error("MONITOR", "CADEIA_INVÁLIDA", "alerta", "hash, índice ou previousHash inconsistente");
        }
    }

    private static void listarSaldos(IMap<String, Block> chainMap) {
        ConsoleLog.subsection("SALDOS DE TOKENS DERIVADOS DA BLOCKCHAIN");
        List<Block> blocks = Blockchain.orderedCopy(chainMap.values());
        if (!Blockchain.isChainValid(blocks)) {
            ConsoleLog.error("MONITOR", "SALDOS_INDISPONÍVEIS", "motivo", "blockchain inválida: não é seguro derivar saldos");
            return;
        }
        Map<String, Integer> saldos = Blockchain.calculateAllSectorBalances(blocks);
        if (saldos.isEmpty()) {
            System.out.println("Nenhum saldo inicializado ainda.");
            return;
        }
        System.out.printf("%-22s %12s%n", "SETOR", "SALDO");
        System.out.println("─".repeat(36));
        saldos.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.printf("%-22s %8d tokens%n", e.getKey(), e.getValue()));
    }

    private static void listarTokensUsados(IMap<String, Block> chainMap) {
        ConsoleLog.subsection("TOKENS DE PAGAMENTO CONFIRMADOS NO LEDGER");
        List<String> tokens = Blockchain.orderedCopy(chainMap.values()).stream()
                .map(Block::getTransaction)
                .filter(tx -> tx != null && tx.getType() == TransactionType.PAYMENT && tx.getTokenId() != null)
                .map(Transaction::getTokenId)
                .sorted()
                .collect(Collectors.toList());
        System.out.println(ConsoleLog.row("Total de pagamentos", String.valueOf(tokens.size())));
        tokens.stream().limit(100).forEach(token -> System.out.println("  • " + token));
        if (tokens.size() > 100) {
            System.out.println("  ... exibindo apenas os 100 primeiros tokens.");
        }
    }

    private static String blockRef(int index) {
        return String.format("#%05d", index);
    }
}
