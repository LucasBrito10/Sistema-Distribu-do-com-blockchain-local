package com.ormuz.client.drone;

import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.interfaces.DroneInterface;
import com.ormuz.shared.security.SectorAuthenticator;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.util.ConsoleLog;

public class Drone extends Client implements DroneInterface {
    private static final long serialVersionUID = 1L;
    private String droneId;
    private boolean inUse;
    private String currentBrokerId;
    private ServicesTypes pendingServiceType = null;
    private String pendingTaskSectorId = null;
    private String pendingMissionId = null;
    private String pendingRouteId = null;
    private String pendingPaymentTokenId = null;
    private String pendingOriginalRequesterId = null;
    private Long activatedAt = null;

    private volatile boolean missionActive = false;
    private volatile String activeMissionId;
    private volatile String activeRouteId;
    private volatile String activeMissionSectorId;

    public Drone(String droneId, boolean inUse, String currentBrokerId) {
        this.droneId = droneId;
        this.inUse = inUse;
        this.currentBrokerId = currentBrokerId;
        this.setId(droneId);
    }

    @Override
    protected void handleIncomingMessage(Message msg) {
        if (msg.getCommandType() == CommandType.ACTIVATE) {
            if (!SectorAuthenticator.verify(msg) || msg.getNodeId() == null || !msg.getNodeId().startsWith("BROKER-")) {
                ConsoleLog.warn("DRONE", "COMANDO_ATIVACAO_REJEITADO", "drone", droneId,
                        "motivo", "assinatura inválida ou origem não autorizada", "origem", msg.getNodeId());
                return;
            }
            if (missionActive) return;
            missionActive = true;
            this.activeMissionId = msg.getMissionId();
            this.activeRouteId = msg.getRouteId();
            this.activeMissionSectorId = msg.getSectorId();
            if (msg.getServiceType() == ServicesTypes.VISUAL_RECONNAISSANCE) {
                visualReconnaissanceService();
            } else if (msg.getServiceType() == ServicesTypes.SEARCH_AND_RESCUE) {
                searchAndRescueService();
            } else if (msg.getServiceType() == ServicesTypes.INFRASTRUCTURE_INSPECTION) {
                infrastructureInspectionService();
            }
        }
    }

    private void executeTask(ServicesTypes service, String taskName) {
        new Thread(() -> {
            try {
                ConsoleLog.info("DRONE", "MISSÃO_INICIADA", "drone", droneId, "missão", activeMissionId, "setor", activeMissionSectorId, "rota", activeRouteId, "tarefa", taskName);
                Thread.sleep(15000);
                ConsoleLog.info("DRONE", "MISSÃO_CONCLUÍDA", "drone", droneId, "missão", activeMissionId, "tarefa", taskName, "status", "retornando à base");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ConsoleLog.warn("DRONE", "MISSÃO_INTERROMPIDA", "drone", droneId, "missão", activeMissionId, "status", "retornando à base");
            } finally {
                missionActive = false;
                String report = Math.random() > 0.15 ? "ROTA_SEGURA" : "OBSTACULO_DETECTADO";
                
                Message completionMsg = new Message();
                completionMsg.setData(0);
                completionMsg.setNodeId(droneId);
                completionMsg.setSectorId(activeMissionSectorId != null ? activeMissionSectorId : getSectorId());
                completionMsg.setServiceType(service);
                completionMsg.setTargetNodeId("BROKER_SIGNAL_COMPLETION");
                completionMsg.setMissionId(activeMissionId);
                completionMsg.setRouteId(activeRouteId);
                completionMsg.setMissionReport(report);
                
                ConsoleLog.info("DRONE", "LAUDO_ENVIADO", "drone", droneId, "missão", activeMissionId, "rota", activeRouteId, "laudo", report);
                this.sendCommandMessage(completionMsg);
                activeMissionId = null;
                activeRouteId = null;
                activeMissionSectorId = null;
            }
        }).start();
    }

    @Override public void visualReconnaissanceService() { executeTask(ServicesTypes.VISUAL_RECONNAISSANCE, "Reconhecimento Visual"); }
    @Override public void searchAndRescueService() { executeTask(ServicesTypes.SEARCH_AND_RESCUE, "Busca e Resgate"); }
    @Override public void infrastructureInspectionService() { executeTask(ServicesTypes.INFRASTRUCTURE_INSPECTION, "Inspeção de Infraestrutura"); }
    
    @Override public void setPendingTaskSectorId(String sectorId) {this.pendingTaskSectorId = sectorId;}
    @Override public String getPendingTaskSectorId() {return this.pendingTaskSectorId;}
    @Override public ServicesTypes getPendingServiceType() {return pendingServiceType;}
    @Override public void setPendingServiceType(ServicesTypes pendingServiceType) {this.pendingServiceType = pendingServiceType;}
    @Override public String getPendingMissionId() { return pendingMissionId; }
    @Override public void setPendingMissionId(String missionId) { this.pendingMissionId = missionId; }
    @Override public String getPendingRouteId() { return pendingRouteId; }
    @Override public void setPendingRouteId(String routeId) { this.pendingRouteId = routeId; }
    @Override public String getPendingPaymentTokenId() { return pendingPaymentTokenId; }
    @Override public void setPendingPaymentTokenId(String tokenId) { this.pendingPaymentTokenId = tokenId; }
    @Override public String getPendingOriginalRequesterId() { return pendingOriginalRequesterId; }
    @Override public void setPendingOriginalRequesterId(String requesterId) { this.pendingOriginalRequesterId = requesterId; }
    @Override public String getDroneId() { return droneId; }
    @Override public void setDroneId(String droneId) { this.droneId = droneId; }
    @Override public boolean isInUse() { return inUse; }
    @Override public void setInUse(boolean inUse) { this.inUse = inUse; }
    @Override public String getCurrentBrokerId() { return currentBrokerId; }
    @Override public void setCurrentBrokerId(String currentBrokerId) { this.currentBrokerId = currentBrokerId; }
    @Override public Long getActivatedAt() { return activatedAt; }
    @Override public void setActivatedAt(Long activatedAt) { this.activatedAt = activatedAt; }
}