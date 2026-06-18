package com.ormuz.client.drone;

import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.interfaces.DroneInterface;

public class DroneBuilder {
    private String droneId;
    private boolean inUse;
    private String currentBrokerId;
    private String host;
    private int port;
    private String sectorId;

    public DroneBuilder withId(String id) { this.droneId = id; return this; }
    public DroneBuilder withSector(String sectorId) { this.sectorId = sectorId; return this; }
    public DroneBuilder withConnection(String host, int port) { this.host = host; this.port = port; return this; }

    public DroneBuilder copyDroneWithStatus(DroneInterface old, boolean inUse, String brokerId) {
        this.droneId = old.getDroneId(); 
        this.inUse = inUse;
        this.currentBrokerId = brokerId;
        return this;
    }

    public Drone build() {
        Drone drone = new Drone(this.droneId, this.inUse, this.currentBrokerId);
        if (host != null) {
            drone.setSectorId(this.sectorId);
            
            // Inicia o Subscriber (para receber os comandos de ativação)
            drone.startSubscribe(host, port, ServicesTypes.VISUAL_RECONNAISSANCE);
            drone.startSubscribe(host, port, ServicesTypes.SEARCH_AND_RESCUE);
            drone.startSubscribe(host, port, ServicesTypes.INFRASTRUCTURE_INSPECTION);
            
            // Inicia o Publisher (para o drone poder avisar que terminou a missão)
            drone.startPublisher(host, port, ServicesTypes.VISUAL_RECONNAISSANCE);
            drone.startPublisher(host, port, ServicesTypes.SEARCH_AND_RESCUE);
            drone.startPublisher(host, port, ServicesTypes.INFRASTRUCTURE_INSPECTION);
        }
        return drone;
    }
}