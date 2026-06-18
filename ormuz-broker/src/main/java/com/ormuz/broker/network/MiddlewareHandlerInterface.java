package com.ormuz.broker.network;

import com.ormuz.shared.types.Message;

public interface MiddlewareHandlerInterface extends Runnable {
    void sendMessage(Message message);
}
