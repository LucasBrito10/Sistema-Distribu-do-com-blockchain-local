package com.ormuz.client.buoy;

import com.ormuz.client.core.Client;
import com.ormuz.shared.enums.ServicesTypes;

class SmartBuoy extends Client implements SmartBuoyInterface {
    @Override
    public void alertEnvironment(ServicesTypes s, int state) { 
        this.sendMessage(state, s); 
    }
}