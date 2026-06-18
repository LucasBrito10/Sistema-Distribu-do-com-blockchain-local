package com.ormuz.broker.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.UUID;

import com.ormuz.broker.core.Broker;
import com.ormuz.broker.core.BrokerBuilder;
import com.ormuz.broker.core.BrokerInterface;
import com.ormuz.broker.log.BrokerLogger;
import com.ormuz.broker.log.BrokerLogger.LogType;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.security.SectorAuthenticator;
import com.ormuz.shared.types.Message;

public class AppBroker {

    public static void main(String[] args) {
        int portaLocal = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        String[] clusterIps = args.length > 1 ? args[1].split(",") : new String[]{"127.0.0.1"};

        String logTypesEnv = System.getenv("BROKER_LOG_TYPES");
        if (logTypesEnv != null && !logTypesEnv.isBlank()) {
            BrokerLogger.desabilitarTodos();
            for (String token : logTypesEnv.split(",")) {
                try {
                    BrokerLogger.habilitar(LogType.valueOf(token.trim().toUpperCase()));
                } catch (IllegalArgumentException e) {
                    BrokerLogger.warn(LogType.SISTEMA, "LOGTYPE_IGNORADO", "valor", token.trim());
                }
            }
        }

        BrokerLogger.event(LogType.SISTEMA, "INICIANDO_BROKER", "porta", String.valueOf(portaLocal));
        BrokerInterface broker = new BrokerBuilder()
                .withPort(portaLocal)
                .withClusterIps(clusterIps)
                .build();
        BrokerLogger.event(LogType.SISTEMA, "BROKER_ONLINE", "porta", String.valueOf(portaLocal));

        new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            try {
                Thread.sleep(7000);
                while (true) {
                    exibirMenu();
                    String op = scanner.nextLine().trim();

                    switch (op) {
                        case "1" -> provisionarRecurso(scanner, broker);
                        case "2" -> reassociarRecursoSetor(scanner, broker);
                        case "3" -> configurarDataTypesSensor(scanner, broker);
                        case "4" -> configurarLogsBroker(scanner);
                        case "5" -> recarregarTokens(scanner, broker);
                        case "6" -> System.exit(0);
                        default  -> BrokerLogger.warn(LogType.SISTEMA, "OPÇÃO_INVÁLIDA", "opção", op);
                    }
                }
            } catch (NoSuchElementException e) {
                BrokerLogger.event(LogType.SISTEMA, "CONSOLE_INDISPONÍVEL", "modo", "background");
            } catch (Exception e) {
                BrokerLogger.err(LogType.SISTEMA, "Erro no painel do broker: " + e.getMessage());
            }
        }).start();

        broker.runServer(true);
    }

    private static void exibirMenu() {
        System.out.println("\n════════════════════════════════════════════════════════════");
        System.out.println("  PAINEL DE CONTROLE DO BROKER ORMUZ");
        System.out.println("════════════════════════════════════════════════════════════");
        System.out.println("  1  Provisionar novo recurso no cluster");
        System.out.println("  2  Reassociar recurso a outro setor");
        System.out.println("  3  Configurar tipos de dado dos sensores");
        System.out.println("  4  Configurar categorias de log do broker");
        System.out.println("  5  Recarregar tokens de um setor (+10)");
        System.out.println("  6  Sair");
        System.out.print("Escolha uma opção: ");
    }

    private static void provisionarRecurso(Scanner scanner, BrokerInterface broker) {
        System.out.println("\n--- Selecione o Tipo de Dispositivo ---");
        System.out.println("1. Radar Costeiro (Coastal Radar)");
        System.out.println("2. Sensor Naval (Naval Sensor)");
        System.out.println("3. Unidade Drone");
        System.out.print("Opcao: ");
        String tipo = scanner.nextLine().trim();

        System.out.print("Setor Alvo (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();

        System.out.print("Broker Alvo para Conexao Fisica (ex: broker-norte): ");
        String brokerAlvo = scanner.nextLine().trim();

        Message cmd = new Message();
        cmd.setCommandType(CommandType.ACTIVATE);
        cmd.setSectorId(setor);
        cmd.setNodeId(brokerAlvo);

        switch (tipo) {
            case "1" -> { cmd.setTargetNodeId("FACTORY-SENSORES-" + setor); cmd.setServiceType(ServicesTypes.TRAFFIC_MONITORING); }
            case "2" -> { cmd.setTargetNodeId("FACTORY-SENSORES-" + setor); cmd.setServiceType(ServicesTypes.HYDROGRAPHIC_PROFILING); }
            case "3" -> { cmd.setTargetNodeId("FACTORY-DRONES-" + setor);   cmd.setServiceType(ServicesTypes.VISUAL_RECONNAISSANCE); }
            default  -> { BrokerLogger.warn(LogType.SISTEMA, "TIPO_RECURSO_INVÁLIDO", "opção", tipo); return; }
        }

        publicarComando(broker, cmd, "Provisonamento de recurso enviado.");
    }

    private static void reassociarRecursoSetor(Scanner scanner, BrokerInterface broker) {
        System.out.println("\n--- Reassociar Recurso a Outro Setor ---");
        System.out.print("ID do Recurso: ");
        String recursoId = scanner.nextLine().trim();

        System.out.print("Novo Setor Alvo (ex: SETOR_SUL): ");
        String novoSetor = scanner.nextLine().trim().toUpperCase();

        System.out.print("Broker do Novo Setor (ex: broker-sul): ");
        String novoBroker = scanner.nextLine().trim();

        Message cmd = new Message();
        cmd.setCommandType(CommandType.REASSIGN_SECTOR);
        cmd.setTargetNodeId(recursoId);
        cmd.setSectorId(novoSetor);
        cmd.setNodeId(novoBroker);

        publicarComando(broker, cmd, "Reassociação de '" + recursoId + "' para setor '" + novoSetor + "' via broker '" + novoBroker + "' enviada.");
    }

    private static void configurarDataTypesSensor(Scanner scanner, BrokerInterface broker) {
        System.out.println("\n--- Configurar Tipos de Dado dos Sensores ---");
        System.out.print("Setor Alvo (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();

        System.out.println("\nTipos de dado disponíveis:");
        ServicesTypes[] todos = ServicesTypes.values();
        for (int i = 0; i < todos.length; i++) {
            System.out.printf("  %2d. %s  (%s)%n", i + 1, todos[i].name(), todos[i].getRelatedResource());
        }
        System.out.println("  Digite os números separados por vírgula para HABILITAR.");
        System.out.println("  Deixe em branco para habilitar TODOS.");
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
                } catch (NumberFormatException e) {}
            }
        }

        if (selecionados.isEmpty()) {
            BrokerLogger.warn(LogType.SISTEMA, "CONFIGURAÇÃO_IGNORADA", "motivo", "nenhum tipo válido selecionado");
            return;
        }

        Message cmd = new Message();
        cmd.setCommandType(CommandType.SET_DATA_TYPES);
        cmd.setTargetNodeId("FACTORY-SENSORES-" + setor);
        cmd.setSectorId(setor);
        cmd.setDataTypes(selecionados);

        publicarComando(broker, cmd, "Configuração de data types para setor '" + setor + "'.");
    }

    private static void configurarLogsBroker(Scanner scanner) {
        System.out.println("\n--- Configurar Logs do Broker ---");
        LogType[] tipos = LogType.values();
        for (int i = 0; i < tipos.length; i++) {
            String estado = BrokerLogger.estaHabilitado(tipos[i]) ? "[ON]" : "[OFF]";
            System.out.printf("  %2d. %s %s%n", i + 1, tipos[i].name(), estado);
        }
        System.out.print("Seleção (vírgula): ");
        String selecao = scanner.nextLine().trim();

        if (selecao.isBlank()) {
            BrokerLogger.habilitarTodos();
            BrokerLogger.event(LogType.SISTEMA, "LOGS_ATUALIZADOS", "categorias", "todas");
            return;
        }

        BrokerLogger.desabilitarTodos();
        for (String parte : selecao.split(",")) {
            try {
                int idx = Integer.parseInt(parte.trim()) - 1;
                if (idx >= 0 && idx < tipos.length) BrokerLogger.habilitar(tipos[idx]);
            } catch (NumberFormatException e) {}
        }
    }

    private static void recarregarTokens(Scanner scanner, BrokerInterface broker) {
        System.out.print("\nSetor para recarregar (ex: SETOR_NORTE): ");
        String setor = scanner.nextLine().trim().toUpperCase();
        Message cmd = new Message();
        cmd.setCommandType(CommandType.ADD_TOKENS);
        cmd.setSectorId(setor);
        cmd.setData(10); // Adiciona +10 tokens
        publicarComando(broker, cmd, "Comando de recarga (+10) enviado para a rede.");
    }

    private static void publicarComando(BrokerInterface broker, Message cmd, String confirmacao) {
        if (broker instanceof Broker b) {
            if (cmd.getCommandType() == CommandType.ADD_TOKENS) {
                // Recarga é uma transação administrativa: deve ser assinada com segredo de admin
                // e processada por um único broker, que cria o bloco e replica via Hazelcast.
                if (cmd.getNodeId() == null || cmd.getNodeId().isBlank()) {
                    cmd.setNodeId("LOCAL-ADMIN-" + b.getBrokerId().substring(0, 8));
                }
                if (cmd.getTokenId() == null || cmd.getTokenId().isBlank()) {
                    cmd.setTokenId("CREDIT-LOCAL-" + UUID.randomUUID());
                }
                SectorAuthenticator.signAdmin(cmd);
                b.processMessage(cmd);
            } else {
                if (cmd.getNodeId() == null || cmd.getNodeId().isBlank()) {
                    cmd.setNodeId("LOCAL-BROKER-" + b.getBrokerId().substring(0, 8));
                }
                if (cmd.getTokenId() == null || cmd.getTokenId().isBlank()) {
                    cmd.setTokenId("CMD-LOCAL-" + UUID.randomUUID());
                }
                if (cmd.getRequestSignature() == null || cmd.getRequestSignature().isBlank()) {
                    SectorAuthenticator.sign(cmd);
                }
                b.getClusterWideBus().publish(cmd);
            }
            BrokerLogger.event(LogType.SISTEMA, "COMANDO_PUBLICADO", "tipo", cmd.getCommandType() != null ? cmd.getCommandType().name() : "N/A", "setor", cmd.getSectorId(), "destino", cmd.getTargetNodeId(), "resumo", confirmacao);
        }
    }
}