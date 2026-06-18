package com.ormuz.client.core;

import java.io.IOException;

public class MiddlewareClientBuilder {
    private String host;
    private int port;
    private String id;

    public MiddlewareClientBuilder withHost(String host) { 
        this.host = host; 
        return this; 
    }
    
    public MiddlewareClientBuilder withPort(int port) { 
        this.port = port; 
        return this; 
    }
    
    public MiddlewareClientBuilder withId(String id) { 
        this.id = id; 
        return this; 
    }

    public MiddlewareClientInterface build() throws IOException {
        return new MiddlewareClient(this.host, this.port, this.id);
    }
}
