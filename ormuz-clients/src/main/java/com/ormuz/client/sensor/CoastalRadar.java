package com.ormuz.client.sensor;

import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.ServicesTypes;

class CoastalRadar extends Client implements CoastalRadarInterface {
    @Override
    public void simulateDetection(ServicesTypes s, int state) { 
        this.sendMessage(state, s); 
    }
}