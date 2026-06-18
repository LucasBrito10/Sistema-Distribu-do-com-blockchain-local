package com.ormuz.client.sensor;

import com.ormuz.shared.enums.ServicesTypes;

public class NavalSensorBuilder {
    private String id;
    private String sectorId;
    private String host;
    private int port;

    public NavalSensorBuilder withId(String id) { this.id = id; return this; }
    public NavalSensorBuilder withSectorId(String sectorId) { this.sectorId = sectorId; return this; }
    public NavalSensorBuilder withConnection(String host, int port) { this.host = host; this.port = port; return this; }

    public NavalSensorInterface build() {
        NavalSensor sensor = new NavalSensor();
        sensor.setId(this.id);
        sensor.setSectorId(this.sectorId);
        // Publishers de telemetria próprios do sensor naval
        sensor.startPublisher(host, port, ServicesTypes.HYDROGRAPHIC_PROFILING);
        sensor.startPublisher(host, port, ServicesTypes.PRESSURE_SENSING);
        // Publisher necessário para requisitar drone de busca e resgate ao broker
        // quando uma leitura instável é detectada (profundidade ou pressão crítica)
        sensor.startPublisher(host, port, ServicesTypes.SEARCH_AND_RESCUE);
        return sensor;
    }
}