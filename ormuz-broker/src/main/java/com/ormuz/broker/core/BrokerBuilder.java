package com.ormuz.broker.core;

public class BrokerBuilder {
    private int port = 8080;
    private String[] clusterIps = new String[]{"127.0.0.1"}; 

    public BrokerBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public BrokerBuilder withClusterIps(String... ips) {
        this.clusterIps = ips;
        return this;
    }
     
    public BrokerInterface build() {
        return new Broker(this.port, this.clusterIps);
    }
}