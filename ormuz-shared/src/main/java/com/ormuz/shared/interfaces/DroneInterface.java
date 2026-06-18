package com.ormuz.shared.interfaces;

import java.io.Serializable;

import com.ormuz.shared.enums.ServicesTypes;

public interface DroneInterface extends Serializable {
    public void visualReconnaissanceService();
    public void searchAndRescueService();
    public void infrastructureInspectionService();
    public String getDroneId();
    public void setDroneId(String droneId);
    public boolean isInUse();
    public void setInUse(boolean inUse);
    public String getCurrentBrokerId();
    public void setCurrentBrokerId(String brokerId);
    // Setor original que requisitou a tarefa, preservado para reassinação em caso de falha
    public String getPendingTaskSectorId();
    public void setPendingTaskSectorId(String sectorId);
    // Tipo de serviço pendente, preservado para reassinação em caso de falha
    public ServicesTypes getPendingServiceType();
    public void setPendingServiceType(ServicesTypes serviceType);
    // Metadados da missão já paga, usados para reatribuição sem nova cobrança.
    public String getPendingMissionId();
    public void setPendingMissionId(String missionId);
    public String getPendingRouteId();
    public void setPendingRouteId(String routeId);
    public String getPendingPaymentTokenId();
    public void setPendingPaymentTokenId(String tokenId);
    public String getPendingOriginalRequesterId();
    public void setPendingOriginalRequesterId(String requesterId);
    // Timestamp (ms) em que o drone foi ativado para uma missão — armazenado no
    // Hazelcast para que qualquer broker do cluster possa fazer o timeout, não apenas
    // o que emitiu o comando ACTIVATE.
    public Long getActivatedAt();
    public void setActivatedAt(Long activatedAt);
}
