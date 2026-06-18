package com.ormuz.broker.log;

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Controle centralizado de logs para todos os brokers do sistema ORMUZ.
 *
 * <p>Formato padronizado:</p>
 * <pre>
 * 2026-06-15 16:42:10.135 | INFO  | BLOCKCHAIN | Bloco #00012 replicado | setor=SETOR_NORTE | saldo=11
 * </pre>
 */
public class BrokerLogger {

    /** Categorias de log disponíveis no broker. */
    public enum LogType {
        /** Inicialização, porta e status geral do servidor. */
        SISTEMA,
        /** Eventos de conexão, desconexão e registro de clientes. */
        CONEXAO,
        /** Entrada e saída de membros no cluster Hazelcast. */
        CLUSTER,
        /** Recebimento e roteamento de mensagens (processMessage). */
        MENSAGEM,
        /** Publicações e recebimentos no canal global (cluster-wide bus). */
        BUS,
        /** Entrega local de mensagens a subscribers deste broker. */
        ENTREGA,
        /** Registro, ativação, liberação e recuperação de drones. */
        DRONE,
        /** Alertas críticos que sempre devem ser visíveis. */
        ALERTA,
        /** Reassociação de recursos entre setores. */
        REASSOCIAR,
        /** Operações de blockchain: mineração de blocos, débitos de tokens e duplo gasto. */
        BLOCKCHAIN
    }

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final int TYPE_WIDTH = 11;
    private static final boolean USE_COLOR = System.getenv("NO_COLOR") == null;

    private static final Map<LogType, String> CORES = new EnumMap<>(LogType.class);
    static {
        CORES.put(LogType.SISTEMA,    "\u001B[36m");
        CORES.put(LogType.CONEXAO,    "\u001B[34m");
        CORES.put(LogType.CLUSTER,    "\u001B[35m");
        CORES.put(LogType.MENSAGEM,   "\u001B[37m");
        CORES.put(LogType.BUS,        "\u001B[33m");
        CORES.put(LogType.ENTREGA,    "\u001B[32m");
        CORES.put(LogType.DRONE,      "\u001B[96m");
        CORES.put(LogType.ALERTA,     "\u001B[91m");
        CORES.put(LogType.REASSOCIAR, "\u001B[93m");
        CORES.put(LogType.BLOCKCHAIN, "\u001B[92m");
    }

    // Por padrão, todos os tipos de log estão habilitados.
    private static final Set<LogType> tiposHabilitados = EnumSet.allOf(LogType.class);

    /** Habilita uma categoria específica de log. */
    public static void habilitar(LogType tipo) { tiposHabilitados.add(tipo); }

    /** Desabilita uma categoria específica de log. */
    public static void desabilitar(LogType tipo) { tiposHabilitados.remove(tipo); }

    /** Habilita todas as categorias de log. */
    public static void habilitarTodos() { tiposHabilitados.addAll(EnumSet.allOf(LogType.class)); }

    /** Desabilita todas as categorias de log. */
    public static void desabilitarTodos() { tiposHabilitados.clear(); }

    /** Retorna true se a categoria informada está habilitada. */
    public static boolean estaHabilitado(LogType tipo) { return tiposHabilitados.contains(tipo); }

    /** Imprime mensagem informativa em stdout, caso a categoria esteja habilitada. */
    public static void log(LogType tipo, String mensagem) {
        print(System.out, "INFO", tipo, mensagem);
    }

    /** Imprime mensagem de erro em stderr, caso a categoria esteja habilitada. */
    public static void err(LogType tipo, String mensagem) {
        print(System.err, "ERRO", tipo, mensagem);
    }

    /** Atalho para mensagens estruturadas por campos chave=valor. */
    public static void event(LogType tipo, String evento, String... campos) {
        print(System.out, "INFO", tipo, appendFields(evento, campos));
    }

    /** Atalho para alertas estruturados por campos chave=valor. */
    public static void warn(LogType tipo, String evento, String... campos) {
        print(System.out, "WARN", tipo, appendFields(evento, campos));
    }

    private static void print(PrintStream out, String level, LogType tipo, String mensagem) {
        if (!tiposHabilitados.contains(tipo)) return;

        String timestamp = LocalDateTime.now().format(FMT);
        String area = rightPad(tipo.name(), TYPE_WIDTH);
        if (USE_COLOR) {
            area = CORES.getOrDefault(tipo, "") + area + "\u001B[0m";
        }

        out.printf("%s | %-5s | %s | %s%n", timestamp, level, area, normalize(mensagem));
    }

    private static String appendFields(String evento, String... campos) {
        StringBuilder sb = new StringBuilder(safe(evento));
        if (campos != null) {
            for (int i = 0; i < campos.length; i += 2) {
                String key = safe(campos[i]);
                String value = (i + 1 < campos.length) ? safe(campos[i + 1]) : "";
                sb.append(" | ").append(key).append("=").append(value);
            }
        }
        return sb.toString();
    }

    private static String normalize(String mensagem) {
        String text = safe(mensagem);
        // Evita repetição visual: a categoria já aparece na coluna AREA.
        text = text.replaceFirst("^\\[(SISTEMA|BLOCKCHAIN|LEDGER)\\]\\s*", "");
        return text;
    }

    private static String rightPad(String text, int width) {
        String safeText = safe(text);
        if (safeText.length() >= width) return safeText;
        return safeText + " ".repeat(width - safeText.length());
    }

    private static String safe(Object value) {
        if (value == null) return "N/A";
        String text = String.valueOf(value).replace('\n', ' ').replace('\r', ' ').trim();
        return text.isBlank() ? "N/A" : text;
    }
}
