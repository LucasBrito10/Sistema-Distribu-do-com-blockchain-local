package com.ormuz.shared.types;

import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.interfaces.DroneInterface;

public class DroneData implements DroneInterface {
    private static final long serialVersionUID = 2L;
    private String droneId;
    private boolean inUse;
    private String currentBrokerId;
    // Setor original que requisitou a tarefa, preservado para reatribuição em caso de falha.
    private String pendingTaskSectorId;
    // Tipo de serviço efetivamente executado pelo drone, preservado para reatribuição.
    private ServicesTypes pendingServiceType;
    // Metadados da missão já paga, usados para reatribuir a missão sem novo débito.
    private String pendingMissionId;
    private String pendingRouteId;
    private String pendingPaymentTokenId;
    private String pendingOriginalRequesterId;
    // Timestamp (ms) em que o drone foi ativado — distribuído via Hazelcast para que
    // qualquer broker do cluster possa aplicar o timeout, não apenas o que ativou.
    private Long activatedAt;

    public DroneData() {}

    public DroneData(String droneId, boolean inUse, String currentBrokerId) {
        this.droneId = droneId;
        this.inUse = inUse;
        this.currentBrokerId = currentBrokerId;
    }

    public DroneData(DroneInterface other) {
        if (other != null) {
            this.droneId = other.getDroneId();
            this.inUse = other.isInUse();
            this.currentBrokerId = other.getCurrentBrokerId();
            this.pendingTaskSectorId = other.getPendingTaskSectorId();
            this.pendingServiceType = other.getPendingServiceType();
            this.pendingMissionId = other.getPendingMissionId();
            this.pendingRouteId = other.getPendingRouteId();
            this.pendingPaymentTokenId = other.getPendingPaymentTokenId();
            this.pendingOriginalRequesterId = other.getPendingOriginalRequesterId();
            this.activatedAt = other.getActivatedAt();
        }
    }

    @Override public void visualReconnaissanceService() {}
    @Override public void searchAndRescueService() {}
    @Override public void infrastructureInspectionService() {}

    @Override public String getDroneId() { return droneId; }
    @Override public void setDroneId(String droneId) { this.droneId = droneId; }
    @Override public boolean isInUse() { return inUse; }
    @Override public void setInUse(boolean inUse) { this.inUse = inUse; }
    @Override public String getCurrentBrokerId() { return currentBrokerId; }
    @Override public void setCurrentBrokerId(String brokerId) { this.currentBrokerId = brokerId; }
    @Override public String getPendingTaskSectorId() { return pendingTaskSectorId; }
    @Override public void setPendingTaskSectorId(String sectorId) { this.pendingTaskSectorId = sectorId; }
    @Override public ServicesTypes getPendingServiceType() { return pendingServiceType; }
    @Override public void setPendingServiceType(ServicesTypes serviceType) { this.pendingServiceType = serviceType; }
    @Override public String getPendingMissionId() { return pendingMissionId; }
    @Override public void setPendingMissionId(String missionId) { this.pendingMissionId = missionId; }
    @Override public String getPendingRouteId() { return pendingRouteId; }
    @Override public void setPendingRouteId(String routeId) { this.pendingRouteId = routeId; }
    @Override public String getPendingPaymentTokenId() { return pendingPaymentTokenId; }
    @Override public void setPendingPaymentTokenId(String tokenId) { this.pendingPaymentTokenId = tokenId; }
    @Override public String getPendingOriginalRequesterId() { return pendingOriginalRequesterId; }
    @Override public void setPendingOriginalRequesterId(String requesterId) { this.pendingOriginalRequesterId = requesterId; }
    @Override public Long getActivatedAt() { return activatedAt; }
    @Override public void setActivatedAt(Long activatedAt) { this.activatedAt = activatedAt; }

    @Override
    public String toString() {
        return "DroneData{id='" + droneId + "', inUse=" + inUse + ", broker='" + currentBrokerId + "'" +
               ", pendingSector='" + pendingTaskSectorId + "', pendingService=" + pendingServiceType +
               ", pendingMission='" + pendingMissionId + "', pendingRoute='" + pendingRouteId + "'" +
               ", pendingPaymentToken='" + pendingPaymentTokenId + "', activatedAt=" + activatedAt + "}";
    }
}
