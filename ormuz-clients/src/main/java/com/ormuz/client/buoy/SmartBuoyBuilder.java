package com.ormuz.client.buoy;

import com.ormuz.shared.enums.ServicesTypes;

public class SmartBuoyBuilder {
    private String id;
    private String sectorId;
    private String host;
    private int port;

    public SmartBuoyBuilder withId(String id) { this.id = id; return this; }
    public SmartBuoyBuilder withSectorId(String s) { this.sectorId = s; return this; }
    public SmartBuoyBuilder withConnection(String h, int p) { this.host = h; this.port = p; return this; }

    public SmartBuoyInterface build() {
        SmartBuoy b = new SmartBuoy();
        b.setId(id); 
        b.setSectorId(sectorId);
        b.startPublisher(host, port, ServicesTypes.WATER_QUALITY_ANALYSIS);
        return b;
    }
}