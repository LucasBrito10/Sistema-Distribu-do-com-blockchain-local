package com.ormuz.client.sensor;

import com.ormuz.shared.enums.ServicesTypes;

public class CoastalRadarBuilder {
    private String id;
    private String sectorId;
    private String host;
    private int port;

    public CoastalRadarBuilder withId(String id) { this.id = id; return this; }
    public CoastalRadarBuilder withSectorId(String sectorId) { this.sectorId = sectorId; return this; }
    public CoastalRadarBuilder withConnection(String host, int port) { this.host = host; this.port = port; return this; }

    public CoastalRadarInterface build() {
        CoastalRadar radar = new CoastalRadar();
        radar.setId(this.id);
        radar.setSectorId(this.sectorId);
        radar.startPublisher(host, port, ServicesTypes.INTRUSION_DETECTION);
        radar.startPublisher(host, port, ServicesTypes.TRAFFIC_MONITORING);
        radar.startPublisher(host, port, ServicesTypes.LONG_RANGE_SURVEILLANCE);
        return radar;
    }
}
