package com.ormuz.broker.network;

import java.net.Socket;
import com.ormuz.broker.core.Broker;

public class MiddlewareHandlerBuilder {
    private Socket socket;
    private Broker broker;

    public MiddlewareHandlerBuilder withSocket(Socket socket) {
        this.socket = socket;
        return this;
    }

    public MiddlewareHandlerBuilder withBroker(Broker broker) {
        this.broker = broker;
        return this;
    }

    public MiddlewareHandlerInterface build() {
        return new MiddlewareHandlerServer(this.socket, this.broker);
    }
}