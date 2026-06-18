package com.ormuz.broker.ledger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.ormuz.shared.types.Block;
import com.ormuz.shared.types.Transaction;

/**
 * Escreve um ledger persistente e legível para as operações de blockchain do
 * broker ORMUZ.
 *
 * <p>O arquivo é aberto em modo <em>append</em>: reinicializações do broker
 * acrescentam entradas ao final sem apagar o histórico existente.</p>
 *
 * <p>Formato de evento:</p>
 * <pre>
 * 2026-06-15 16:42:10 | DEBIT_OK    | bloco=#00007 | setor=SETOR_NORTE | saldo=11
 *     serviço   : INTRUSION_DETECTION
 *     token     : REQ-RDR-...
 *     hash      : 19ab...
 *     anterior  : 95fe...
 * </pre>
 */
public class LedgerWriter implements AutoCloseable {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int EVENT_WIDTH = 13;
    private static final int DETAIL_KEY_WIDTH = 12;

    private final BufferedWriter writer;
    private final String brokerIdShort;
    private final String filePath;
    private boolean closed;

    /**
     * Abre (ou cria) o arquivo de ledger e escreve o cabeçalho de sessão.
     *
     * @param brokerId UUID completo do broker
     * @throws IOException se o arquivo não puder ser aberto
     */
    public LedgerWriter(String brokerId) throws IOException {
        this.brokerIdShort = brokerId != null && brokerId.length() >= 8 ? brokerId.substring(0, 8) : "UNKNOWN";
        this.filePath = resolveFilePath(brokerIdShort);

        Path path = Paths.get(this.filePath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        this.writer = new BufferedWriter(new FileWriter(this.filePath, true));
        writeSessionHeader(brokerId, this.filePath);
    }

    /** Registra um bloco minerado com sucesso (débito de token aprovado). */
    public synchronized void recordBlock(int blockIndex, String sector, String service,
                                         String tokenId, int balanceAfter,
                                         String hash, String prevHash) {
        writeEvent(
                "DEBIT_OK",
                String.format("bloco=%s | setor=%s | saldo=%d", blockRef(blockIndex), nvl(sector), balanceAfter),
                "serviço", service,
                "token", tokenId,
                "hash", abbrev(hash, 24),
                "anterior", abbrev(prevHash, 24)
        );
    }

    /** Registra tentativa de duplo gasto detectada. */
    public synchronized void recordDoubleSpend(String sector, String tokenId) {
        writeEvent(
                "DOUBLE_SPEND",
                String.format("setor=%s | token rejeitado", nvl(sector)),
                "token", tokenId,
                "ação", "pagamento recusado e auditoria gravada na blockchain"
        );
    }

    /** Registra rejeição por saldo insuficiente. */
    public synchronized void recordInsufficientBalance(String sector, Integer balance) {
        writeEvent(
                "NO_FUNDS",
                String.format("setor=%s | saldo=%d", nvl(sector), balance == null ? 0 : balance),
                "ação", "requisição recusada por saldo insuficiente"
        );
    }

    /** Registra falha por esgotamento de tentativas/consenso. */
    public synchronized void recordCasFailure(String sector) {
        writeEvent(
                "CAS_FAIL",
                String.format("setor=%s", nvl(sector)),
                "ação", "falha ao concluir gravação atômica do bloco"
        );
    }

    /** Registra a adição de saldo a um setor (crédito de tokens). */
    public synchronized void recordCredit(String sector, int tokensAdded, int newBalance) {
        writeEvent(
                "CREDIT",
                String.format("setor=%s | +%d tokens | saldo=%d", nvl(sector), tokensAdded, newBalance),
                "origem", "LOCAL_ADMIN",
                "ação", "recarga registrada na blockchain"
        );
    }

    /** Registra um laudo de missão gravado na blockchain. */
    public synchronized void recordMissionReport(int blockIndex, String droneId, String sector,
                                                 String service, String route, String report,
                                                 String hash, String prevHash) {
        writeEvent(
                "MISSION_LOG",
                String.format("bloco=%s | setor=%s | drone=%s", blockRef(blockIndex), nvl(sector), nvl(droneId)),
                "serviço", service,
                "rota", route,
                "laudo", report,
                "hash", abbrev(hash, 24),
                "anterior", abbrev(prevHash, 24)
        );
    }


    /** Registra que este broker passou a possuir uma cópia local de um bloco.
     *  O evento é escrito uma única vez por hash pelo Broker, inclusive para blocos
     *  produzidos em outros nós e recebidos por replicação. */
    public synchronized void recordBlockchainCopy(Block block, String origin, boolean chainValid) {
        if (block == null) return;
        Transaction tx = block.getTransaction();
        writeEvent(
                "CHAIN_COPY",
                String.format("bloco=%s | origem=%s | cadeia=%s", blockRef(block.getIndex()), nvl(origin), chainValid ? "válida" : "inválida"),
                "tipo", tx != null && tx.getType() != null ? tx.getType().name() : "N/A",
                "setor origem", tx != null ? tx.getFromSector() : "N/A",
                "setor destino", tx != null ? tx.getToSector() : "N/A",
                "serviço", tx != null ? tx.getServiceType() : "N/A",
                "token", tx != null ? tx.getTokenId() : "N/A",
                "missão", tx != null ? tx.getMissionId() : "N/A",
                "hash", abbrev(block.getHash(), 24),
                "anterior", abbrev(block.getPreviousHash(), 24)
        );
    }

    /** Registra a gravação do snapshot local completo da cadeia em arquivo JSONL. */
    public synchronized void recordChainSnapshot(Path snapshotFile, int blockCount, boolean chainValid) {
        writeEvent(
                "CHAIN_FILE",
                String.format("blocos=%d | cadeia=%s", blockCount, chainValid ? "válida" : "inválida"),
                "arquivo", snapshotFile != null ? snapshotFile.toString() : "N/A",
                "ação", "snapshot local completo regravado"
        );
    }

    /** Caminho do ledger textual em uso por este broker. */
    public String getFilePath() {
        return filePath;
    }

    /** Escreve entrada de inicialização (após o broker ter seu ID definido). */
    public synchronized void recordStartup(String brokerIdFull) {
        writeEvent(
                "STARTUP",
                "broker=" + brokerIdShort,
                "broker completo", brokerIdFull,
                "status", "ledger pronto para auditoria"
        );
    }

    /** Escreve entrada de encerramento e fecha o arquivo. */
    @Override
    public synchronized void close() {
        if (closed) return;
        writeEvent("SHUTDOWN", "broker=" + brokerIdShort, "status", "sessão encerrada");
        writeLine(separator());
        try {
            writer.close();
        } catch (IOException e) {
            System.err.println("[LEDGER] Erro ao fechar o ledger: " + e.getMessage());
        } finally {
            closed = true;
        }
    }

    private void writeSessionHeader(String brokerId, String filePath) {
        writeLine(separator());
        writeLine("# ORMUZ LEDGER DE AUDITORIA");
        writeLine("# broker curto : " + brokerIdShort);
        writeLine("# broker UUID  : " + nvl(brokerId));
        writeLine("# início       : " + now());
        writeLine("# arquivo      : " + filePath);
        writeLine("# formato      : timestamp | evento | resumo + detalhes indentados");
        writeLine(separator());
    }

    private void writeEvent(String event, String summary, String... details) {
        writeLine(String.format("%s | %-" + EVENT_WIDTH + "s | %s", now(), nvl(event), nvl(summary)));
        if (details != null) {
            for (int i = 0; i < details.length; i += 2) {
                String key = nvl(details[i]);
                String value = (i + 1 < details.length) ? nvl(details[i + 1]) : "";
                writeLine(String.format("    %-" + DETAIL_KEY_WIDTH + "s: %s", key, value));
            }
        }
    }

    private void writeLine(String line) {
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("[LEDGER] Falha ao escrever entrada: " + e.getMessage());
        }
    }

    private static String resolveFilePath(String brokerIdShort) {
        String env = System.getenv("LEDGER_FILE");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        return "ledger-" + brokerIdShort + ".log";
    }

    private static String now() {
        return LocalDateTime.now().format(FMT);
    }

    private static String blockRef(int blockIndex) {
        return String.format("#%05d", blockIndex);
    }

    /** Retorna prefixo de {@code s} com {@code "..."} se for mais longo que {@code len}. */
    private static String abbrev(String s, int len) {
        if (s == null || s.isBlank()) return "N/A";
        return s.length() > len ? s.substring(0, len) + "..." : s;
    }

    /** Substitui null por {@code "N/A"}. */
    private static String nvl(Object value) {
        if (value == null) return "N/A";
        String text = String.valueOf(value).replace('\n', ' ').replace('\r', ' ').trim();
        return text.isBlank() ? "N/A" : text;
    }

    private static String separator() {
        return "#" + "═".repeat(118);
    }
}
