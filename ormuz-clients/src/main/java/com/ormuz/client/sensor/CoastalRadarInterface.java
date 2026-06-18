package com.ormuz.client.sensor;

import com.ormuz.shared.enums.ServicesTypes;

public interface CoastalRadarInterface {
    void simulateDetection(ServicesTypes s, int state);
}