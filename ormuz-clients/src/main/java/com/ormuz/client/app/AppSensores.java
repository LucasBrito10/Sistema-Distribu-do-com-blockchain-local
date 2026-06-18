package com.ormuz.client.app;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ormuz.client.core.Client;
import com.ormuz.client.sensor.CoastalRadarBuilder;
import com.ormuz.client.sensor.CoastalRadarInterface;
import com.ormuz.client.sensor.NavalSensorBuilder;
import com.ormuz.client.sensor.NavalSensorInterface;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.util.ConsoleLog;

public class AppSensores extends Client {

    private static final int THRESHOLD_PROFUNDIDADE   = 18;  
    private static final int THRESHOLD_PRESSAO        = 130; 
    private static final int THRESHOLD_INTRUSAO       = 0;   
    private static final int THRESHOLD_TRAFEGO        = 0;   
    private static final int THRESHOLD_VIGILANCIA     = 0;   

    private static final Set<ServicesTypes> servicosAtivos = EnumSet.allOf(ServicesTypes.class);

    private static String ipBrokerGlobal;
    private static int    portaBrokerGlobal;
    private static String setorGlobal;

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg.getCommandType() == CommandType.ACTIVATE) {
            if (msg.getServiceType() == ServicesTypes.TRAFFIC_MONITORING) {
                inicializarRadar(ipBrokerGlobal, portaBrokerGlobal, setorGlobal);
            } else if (msg.getServiceType() == ServicesTypes.HYDROGRAPHIC_PROFILING) {
                inicializarSensorNaval(ipBrokerGlobal, portaBrokerGlobal, setorGlobal);
            }
        } else if (msg.getCommandType() == CommandType.SET_DATA_TYPES) {
            aplicarConfiguracaoDataTypes(msg.getDataTypes());
        } else if (msg.getCommandType() == CommandType.REASSIGN_SECTOR && getId().equals(msg.getTargetNodeId())) {
            String novoSetor  = msg.getSectorId();
            String novoBroker = msg.getNodeId();
            logSistema(setorGlobal, "Fábrica de sensores reassociada para setor " + novoSetor + " (broker: " + novoBroker + ")");
            setorGlobal = novoSetor;
            String[] partes = ipBrokerGlobal.split(",");
            partes[0] = novoBroker;
            ipBrokerGlobal = String.join(",", partes);
            setSectorId(novoSetor);
        } else if (msg.getCommandType() == CommandType.REQUEST_STATUS) {
            imprimirStatusDataTypes();
        } else if (msg.getCommandType() == CommandType.PAYMENT_REJECTED && setorGlobal.equals(msg.getSectorId())) {
            String motivo = msg.getMissionReport() != null && !msg.getMissionReport().isBlank()
                    ? msg.getMissionReport()
                    : "Saldo de tokens insuficiente ou duplo gasto";
            logInstavel(setorGlobal, "SISTEMA", "FINANCEIRO", "Requisição REJEITADA: " + motivo + ".");
        }
    }

    private static boolean isDadoInstavel(ServicesTypes servico, int valor) {
        return switch (servico) {
            case HYDROGRAPHIC_PROFILING  -> valor > THRESHOLD_PROFUNDIDADE;
            case PRESSURE_SENSING        -> valor > THRESHOLD_PRESSAO;
            case INTRUSION_DETECTION     -> valor > THRESHOLD_INTRUSAO;
            case TRAFFIC_MONITORING      -> valor > THRESHOLD_TRAFEGO;
            case LONG_RANGE_SURVEILLANCE -> valor > THRESHOLD_VIGILANCIA;
            default                      -> false;
        };
    }

    private static String descricaoInstabilidade(ServicesTypes servico, int valor) {
        return switch (servico) {
            case HYDROGRAPHIC_PROFILING  -> "Profundidade crítica: " + valor + "m (limite: " + THRESHOLD_PROFUNDIDADE + "m)";
            case PRESSURE_SENSING        -> "Pressão fora da faixa: " + valor + " kPa (limite: " + THRESHOLD_PRESSAO + " kPa)";
            case INTRUSION_DETECTION     -> "Embarcação hostil detectada!";
            case TRAFFIC_MONITORING      -> "Tráfego marítimo congestionado!";
            case LONG_RANGE_SURVEILLANCE -> "Alerta de vigilância de longo alcance!";
            default                      -> "Leitura anômala: " + valor;
        };
    }

    private static ServicesTypes droneParaServico(ServicesTypes servico) {
        return switch (servico) {
            case INTRUSION_DETECTION, TRAFFIC_MONITORING, LONG_RANGE_SURVEILLANCE -> ServicesTypes.INTRUSION_DETECTION; 
            case HYDROGRAPHIC_PROFILING, PRESSURE_SENSING -> ServicesTypes.SEARCH_AND_RESCUE;   
            default -> ServicesTypes.INTRUSION_DETECTION;
        };
    }

    private static void logEstavel(String setor, String nodeType, String dataType, String detalhe) {
        ConsoleLog.info("SENSOR", "LEITURA_ESTÁVEL", "setor", setor, "nó", nodeType, "dado", dataType, "detalhe", detalhe);
    }

    private static void logInstavel(String setor, String nodeType, String dataType, String motivo) {
        ConsoleLog.warn("SENSOR", "LEITURA_INSTÁVEL", "setor", setor, "nó", nodeType, "dado", dataType, "motivo", motivo);
    }

    private static void logDroneRequisitado(String setor, String nodeType, ServicesTypes tipoServicoDrone) {
        ConsoleLog.info("SENSOR", "DRONE_REQUISITADO", "setor", setor, "nó", nodeType, "serviço drone", tipoServicoDrone.name());
    }

    private static void logSistema(String setor, String msg) {
        ConsoleLog.info("SENSOR", "SISTEMA", "setor", setor, "mensagem", msg);
    }

    private static void emitirDado(Object sensor, ServicesTypes servico, int valor, String setor, String nodeType) {
        boolean instavel = isDadoInstavel(servico, valor);

        if (instavel) {
            logInstavel(setor, nodeType, servico.name(), descricaoInstabilidade(servico, valor));
            ServicesTypes tipoServicoDrone = droneParaServico(servico);
            logDroneRequisitado(setor, nodeType, tipoServicoDrone);
            
            if (sensor instanceof CoastalRadarInterface r) {
                r.simulateDetection(servico, valor);
                r.simulateDetection(tipoServicoDrone, 1);
            } else if (sensor instanceof NavalSensorInterface n) {
                n.sendTelemetry(servico, valor);
                n.sendTelemetry(tipoServicoDrone, 1);
            }
        } else {
            String detalhe = switch (servico) {
                case HYDROGRAPHIC_PROFILING  -> "Profundidade: " + valor + "m";
                case PRESSURE_SENSING        -> "Pressao: " + valor + " kPa";
                case TRAFFIC_MONITORING      -> "Trafego registrado";
                case INTRUSION_DETECTION     -> "Area monitorada, sem anomalias";
                case LONG_RANGE_SURVEILLANCE -> "Varredura concluida sem alertas";
                default                      -> "Valor: " + valor;
            };
            logEstavel(setor, nodeType, servico.name(), detalhe);
            
            if (sensor instanceof CoastalRadarInterface r) {
                r.simulateDetection(servico, valor);
            } else if (sensor instanceof NavalSensorInterface n) {
                n.sendTelemetry(servico, valor);
            }
        }
    }

    private void inicializarRadar(String host, int port, String setor) {
        CoastalRadarInterface radar = new CoastalRadarBuilder()
                .withId("RDR-" + setor + "-" + (System.currentTimeMillis() % 1000))
                .withSectorId(setor)
                .withConnection(host, port)
                .build();

        Thread t = new Thread(() -> {
            int ciclo = 0;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(15000);
                    if (ciclo > 0 && ciclo % 5 == 0) {
                        synchronized (AppSensores.class) {
                            boolean simularInstabilidade = Math.random() < 0.30;
                            if (servicosAtivos.contains(ServicesTypes.INTRUSION_DETECTION)) {
                                emitirDado(radar, ServicesTypes.INTRUSION_DETECTION, simularInstabilidade ? 1 : 0, setor, "RADAR_COSTEIRO");
                            }
                            if (servicosAtivos.contains(ServicesTypes.TRAFFIC_MONITORING)) {
                                emitirDado(radar, ServicesTypes.TRAFFIC_MONITORING, simularInstabilidade ? 1 : 0, setor, "RADAR_COSTEIRO");
                            }
                            if (servicosAtivos.contains(ServicesTypes.LONG_RANGE_SURVEILLANCE)) {
                                emitirDado(radar, ServicesTypes.LONG_RANGE_SURVEILLANCE, simularInstabilidade ? 1 : 0, setor, "RADAR_COSTEIRO");
                            }
                        }
                    }
                    ciclo++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.setDaemon(true);
        t.start();
        logSistema(setor, "Instancia de RADAR COSTEIRO alocada com suporte a cluster-failover.");
    }

    private void inicializarSensorNaval(String host, int port, String setor) {
        NavalSensorInterface sensorNaval = new NavalSensorBuilder()
                .withId("SNV-" + setor + "-" + (System.currentTimeMillis() % 1000))
                .withSectorId(setor)
                .withConnection(host, port)
                .build();

        Thread t = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(15000);
                    synchronized (AppSensores.class) {
                        boolean simularInstabilidade = Math.random() < 0.25;
                        if (servicosAtivos.contains(ServicesTypes.HYDROGRAPHIC_PROFILING)) {
                            int profundidade = simularInstabilidade ? 19 + (int)(Math.random() * 6) : 15 + (int)(Math.random() * 3);
                            emitirDado(sensorNaval, ServicesTypes.HYDROGRAPHIC_PROFILING, profundidade, setor, "SENSOR_NAVAL");
                        }
                        if (servicosAtivos.contains(ServicesTypes.PRESSURE_SENSING)) {
                            int pressao = simularInstabilidade ? 131 + (int)(Math.random() * 29) : 100 + (int)(Math.random() * 30);
                            emitirDado(sensorNaval, ServicesTypes.PRESSURE_SENSING, pressao, setor, "SENSOR_NAVAL");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.setDaemon(true);
        t.start();
        logSistema(setor, "Instancia de SENSOR NAVAL alocada com suporte a cluster-failover.");
    }

    private static synchronized void aplicarConfiguracaoDataTypes(List<ServicesTypes> novosAtivos) {
        if (novosAtivos == null || novosAtivos.isEmpty()) return;
        servicosAtivos.clear();
        servicosAtivos.addAll(novosAtivos);
        String lista = novosAtivos.stream().map(ServicesTypes::name).collect(Collectors.joining(", "));
        logSistema(setorGlobal, "[CONTROLE] Tipos de dado ativos atualizados: [" + lista + "]");
    }

    private static synchronized void imprimirStatusDataTypes() {
        String lista = servicosAtivos.stream().map(ServicesTypes::name).collect(Collectors.joining(", "));
        logSistema(setorGlobal, "[STATUS] Tipos de dado ativos: [" + lista + "]");
    }

    public static void main(String[] args) throws InterruptedException {
        ipBrokerGlobal    = args.length > 0 ? args[0] : "127.0.0.1";
        portaBrokerGlobal = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        setorGlobal       = args.length > 2 ? args[2] : "SETOR_NORTE";

        AppSensores fabrica = new AppSensores();
        fabrica.setId("FACTORY-SENSORES-" + setorGlobal);
        fabrica.setSectorId(setorGlobal);

        fabrica.startSubscribe(ipBrokerGlobal, portaBrokerGlobal, ServicesTypes.TRAFFIC_MONITORING);
        fabrica.startSubscribe(ipBrokerGlobal, portaBrokerGlobal, ServicesTypes.HYDROGRAPHIC_PROFILING);

        logSistema(setorGlobal, "Container de provisionamento de Sensores online.");
        while (true) { Thread.sleep(60000); }
    }
}