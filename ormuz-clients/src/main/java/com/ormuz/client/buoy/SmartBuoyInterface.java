package com.ormuz.client.buoy;

import com.ormuz.shared.enums.ServicesTypes;

public interface SmartBuoyInterface {
    void alertEnvironment(ServicesTypes s, int state);
}