package com.ormuz.client.core;

import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.types.Message;

public interface MiddlewareClientInterface extends Runnable {
    void publish();
    void subscribe();
    void sendMessage(Message m);
    Message receiveMessageFromQueue() throws InterruptedException;
    void setServiceType(ServicesTypes s);
    void setSectorId(String s);
}
