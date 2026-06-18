package com.ormuz.client.sensor;

import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.ServicesTypes;

class NavalSensor extends Client implements NavalSensorInterface {
    @Override
    public void sendTelemetry(ServicesTypes s, int state) { 
        this.sendMessage(state, s); 
    }
}