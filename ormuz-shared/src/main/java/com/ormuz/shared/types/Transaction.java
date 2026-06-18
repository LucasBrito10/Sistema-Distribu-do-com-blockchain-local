package com.ormuz.shared.types;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

import com.ormuz.shared.enums.TransactionType;

/**
 * Evento financeiro/operacional gravado dentro de um bloco.
 *
 * <p>No Problema 3 o ativo é o saldo operacional de cada setor. Portanto,
 * não há a ideia de companhias ou transferência entre companhias: sensores
 * de um setor fazem requisições e a requisição só é concluída se o histórico
 * imutável da blockchain mostrar saldo suficiente para aquele setor.</p>
 */
public class Transaction implements Serializable {
    private static final long serialVersionUID = 2L;

    private String transactionId;
    private TransactionType type;
    private String tokenId;
    private String fromSector;
    private String toSector;
    private int amount;
    private String serviceType;
    private long timestamp;

    private String requestNodeId;
    private long requestTimestamp;
    private String requestSignature;

    private String missionId;
    private String droneId;
    private String routeId;
    private String missionReport;

    public Transaction() {}

    /** Compatibilidade com o código antigo: transação de pagamento unitário. */
    public Transaction(String tokenId, String fromSector, String serviceType, long timestamp) {
        this(tokenId, fromSector, serviceType, timestamp, null);
    }

    /** Compatibilidade com o código antigo: pagamento ou laudo, dependendo do serviço. */
    public Transaction(String tokenId, String fromSector, String serviceType, long timestamp, String missionReport) {
        this.transactionId = tokenId != null ? tokenId : UUID.randomUUID().toString();
        this.tokenId       = tokenId;
        this.fromSector    = fromSector;
        this.serviceType   = serviceType;
        this.timestamp     = timestamp;
        this.missionReport = missionReport;
        if ("GENESIS".equals(serviceType)) {
            this.type = TransactionType.GENESIS;
            this.amount = 0;
        } else if (serviceType != null && serviceType.startsWith("MISSION_LOG")) {
            this.type = TransactionType.MISSION_LOG;
            this.amount = 0;
        } else {
            this.type = TransactionType.PAYMENT;
            this.amount = 1;
        }
    }

    public static Transaction genesis() {
        Transaction tx = new Transaction("GENESIS-TOKEN", "SYSTEM", "GENESIS", 0L);
        tx.setTransactionId("GENESIS");
        tx.setType(TransactionType.GENESIS);
        tx.setMissionReport("CADEIA_INICIALIZADA");
        return tx;
    }

    public static Transaction issue(String sector, int amount, String reason) {
        Transaction tx = new Transaction();
        tx.setTransactionId("ISSUE-" + sector + "-" + amount + "-" + Math.abs(Objects.hash(sector, amount, reason)));
        tx.setType(TransactionType.ISSUE);
        tx.setFromSector("SYSTEM");
        tx.setToSector(sector);
        tx.setAmount(amount);
        tx.setServiceType("ISSUE");
        tx.setTimestamp(System.currentTimeMillis());
        tx.setMissionReport(reason);
        return tx;
    }

    public static Transaction credit(String sector, int amount, String requestNodeId, String signature) {
        Transaction tx = new Transaction();
        tx.setTransactionId("CREDIT-" + UUID.randomUUID());
        tx.setType(TransactionType.CREDIT);
        tx.setFromSector("SYSTEM");
        tx.setToSector(sector);
        tx.setAmount(amount);
        tx.setServiceType("CREDIT_RECHARGE");
        tx.setTimestamp(System.currentTimeMillis());
        tx.setRequestNodeId(requestNodeId);
        tx.setRequestSignature(signature);
        return tx;
    }

    public static Transaction payment(String tokenId, String sector, String serviceType,
                                      String requestNodeId, long requestTimestamp, String signature) {
        Transaction tx = new Transaction();
        tx.setTransactionId("PAY-" + tokenId);
        tx.setType(TransactionType.PAYMENT);
        tx.setTokenId(tokenId);
        tx.setFromSector(sector);
        tx.setToSector("ORMUZ_ESCORT_SERVICE");
        tx.setAmount(1);
        tx.setServiceType(serviceType);
        tx.setTimestamp(System.currentTimeMillis());
        tx.setRequestNodeId(requestNodeId);
        tx.setRequestTimestamp(requestTimestamp);
        tx.setRequestSignature(signature);
        return tx;
    }

    public static Transaction rejectedPayment(String tokenId, String sector, String serviceType,
                                              String requestNodeId, String reason) {
        Transaction tx = new Transaction();
        tx.setTransactionId("REJECT-" + UUID.randomUUID());
        tx.setType(TransactionType.PAYMENT_REJECTED);
        tx.setTokenId(tokenId);
        tx.setFromSector(sector);
        tx.setToSector("ORMUZ_ESCORT_SERVICE");
        tx.setAmount(0);
        tx.setServiceType(serviceType);
        tx.setTimestamp(System.currentTimeMillis());
        tx.setRequestNodeId(requestNodeId);
        tx.setMissionReport(reason);
        return tx;
    }

    public static Transaction missionLog(String missionId, String sector, String serviceType,
                                         String droneId, String routeId, String report) {
        Transaction tx = new Transaction();
        tx.setTransactionId("LOG-" + (missionId != null && !missionId.isBlank() ? missionId : UUID.randomUUID().toString()));
        tx.setType(TransactionType.MISSION_LOG);
        tx.setTokenId("MISSION-" + UUID.randomUUID());
        tx.setFromSector(sector);
        tx.setToSector("ORMUZ_AUDIT_LEDGER");
        tx.setAmount(0);
        tx.setServiceType(serviceType);
        tx.setTimestamp(System.currentTimeMillis());
        tx.setMissionId(missionId);
        tx.setDroneId(droneId);
        tx.setRouteId(routeId);
        tx.setMissionReport(report);
        return tx;
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public TransactionType getType() { return type; }
    public void setType(TransactionType type) { this.type = type; }

    public String getTokenId() { return tokenId; }
    public void setTokenId(String tokenId) { this.tokenId = tokenId; }

    public String getFromSector() { return fromSector; }
    public void setFromSector(String fromSector) { this.fromSector = fromSector; }

    public String getToSector() { return toSector; }
    public void setToSector(String toSector) { this.toSector = toSector; }

    public int getAmount() { return amount; }
    public void setAmount(int amount) { this.amount = amount; }

    public String getServiceType() { return serviceType; }
    public void setServiceType(String serviceType) { this.serviceType = serviceType; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getRequestNodeId() { return requestNodeId; }
    public void setRequestNodeId(String requestNodeId) { this.requestNodeId = requestNodeId; }

    public long getRequestTimestamp() { return requestTimestamp; }
    public void setRequestTimestamp(long requestTimestamp) { this.requestTimestamp = requestTimestamp; }

    public String getRequestSignature() { return requestSignature; }
    public void setRequestSignature(String requestSignature) { this.requestSignature = requestSignature; }

    public String getMissionId() { return missionId; }
    public void setMissionId(String missionId) { this.missionId = missionId; }

    public String getDroneId() { return droneId; }
    public void setDroneId(String droneId) { this.droneId = droneId; }

    public String getRouteId() { return routeId; }
    public void setRouteId(String routeId) { this.routeId = routeId; }

    public String getMissionReport() { return missionReport; }
    public void setMissionReport(String missionReport) { this.missionReport = missionReport; }

    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + transactionId + '\'' +
                ", type=" + type +
                ", token='" + tokenId + '\'' +
                ", from='" + fromSector + '\'' +
                ", to='" + toSector + '\'' +
                ", amount=" + amount +
                ", service='" + serviceType + '\'' +
                ", node='" + requestNodeId + '\'' +
                ", mission='" + missionId + '\'' +
                ", drone='" + droneId + '\'' +
                ", route='" + routeId + '\'' +
                ", report='" + missionReport + '\'' +
                ", ts=" + timestamp +
                '}';
    }
}
