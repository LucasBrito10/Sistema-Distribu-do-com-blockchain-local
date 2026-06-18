package com.ormuz.client.sensor;

import com.ormuz.shared.enums.ServicesTypes;

public interface NavalSensorInterface {
    void sendTelemetry(ServicesTypes s, int state);
}
