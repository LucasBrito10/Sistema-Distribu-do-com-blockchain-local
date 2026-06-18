package com.ormuz.client.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.util.ConsoleLog;

class MiddlewareClient implements MiddlewareClientInterface {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String[] hosts;
    private int currentHostIdx = 0;
    private final int port;
    private final String idClient;
    
    private Socket socket;
    private BufferedReader in;
    private volatile PrintWriter out;
    
    private ServicesTypes serviceType;
    private String sectorId;
    private String connectionType;

    private final BlockingQueue<Message> sendQueue    = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> receiveQueue = new LinkedBlockingQueue<>();

    MiddlewareClient(String clusterIps, int port, String id) {
        this.idClient = id;
        this.port     = port;
        
        // Divide a string de IPs/Hosts do cluster em um array e remove duplicatas,
        // preservando a ordem de inserção (o primeiro elemento — broker preferencial
        // do setor — permanece na posição 0 mesmo que apareça novamente na lista
        // de fallback enviada pelo docker-compose).
        if (clusterIps != null && !clusterIps.isBlank()) {
            List<String> seen = new ArrayList<>(new LinkedHashSet<>(
                List.of(clusterIps.split(","))
            ));
            // Remove entradas em branco que podem surgir se PUBLIC_ADDRESS não foi definido
            seen.removeIf(String::isBlank);
            this.hosts = seen.toArray(new String[0]);
        } else {
            this.hosts = new String[]{"127.0.0.1"};
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (in == null) {
                    Thread.sleep(1000);
                    continue;
                }
                String json = in.readLine();
                if (json == null) {
                    throw new IOException("Fim de stream atingido. Broker desconectado.");
                }
                Message m = MAPPER.readValue(json, Message.class);
                if (m != null) receiveQueue.put(m);
            } catch (IOException e) {
                ConsoleLog.warn("REDE", "CONEXÃO_PERDIDA", "cliente", idClient, "broker", hosts[currentHostIdx].trim(), "ação", "buscando fallback");
                closeCurrentSocket();
                currentHostIdx = (currentHostIdx + 1) % hosts.length;
                connectAndRegister();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private synchronized void closeCurrentSocket() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ignored) {}
        this.in = null;
        this.out = null;
        this.socket = null;
    }

    //Handshake de conexão e registro no Broker, com lógica de failover para múltiplos hosts
    private synchronized void connectAndRegister() {
        while (!Thread.currentThread().isInterrupted()) {
            String targetHost = hosts[currentHostIdx].trim();
            try {
                ConsoleLog.info("REDE", "CONECTANDO", "cliente", idClient, "broker", targetHost + ":" + port, "tipo", connectionType);
                this.socket = new Socket(targetHost, port);
                this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
                this.out = new PrintWriter(this.socket.getOutputStream(), true);

                // Reenvia o contrato de identidade (Publish/Subscribe) para o novo Broker
                Message reg = new Message();
                reg.setNodeId(idClient);
                reg.setConnectionType(connectionType);
                reg.setServiceType(serviceType);
                reg.setSectorId(sectorId);
                
                this.out.println(MAPPER.writeValueAsString(reg));
                ConsoleLog.info("REDE", "REGISTRADO", "cliente", idClient, "broker", targetHost, "tipo", connectionType, "serviço", serviceType != null ? serviceType.name() : "N/A");
                return; 
            } catch (IOException e) {
                ConsoleLog.warn("REDE", "BROKER_INDISPONÍVEL", "cliente", idClient, "broker", targetHost, "próxima tentativa", "5s");
                currentHostIdx = (currentHostIdx + 1) % hosts.length;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    @Override
    public void publish() {
        this.connectionType = "PUBLISHER";
        connectAndRegister();

        Thread sendThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Message m = sendQueue.take();
                    boolean sended = false;
                    while (!sended && !Thread.currentThread().isInterrupted()) {
                        PrintWriter currentOut = this.out;
                        if (currentOut != null) {
                            currentOut.println(MAPPER.writeValueAsString(m));
                            if (currentOut.checkError()) {
                                ConsoleLog.warn("REDE", "FALHA_DE_ESCRITA", "cliente", idClient, "ação", "forçando failover");
                                closeCurrentSocket();
                            } else {
                                sended = true;
                            }
                        }
                        if (!sended) {
                            Thread.sleep(2000); // Aguarda o loop de reconexão reestabelecer o PrintWriter
                        }
                    }
                }
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        sendThread.setDaemon(true);
        sendThread.start();

        Thread readThread = new Thread(this);
        readThread.setDaemon(true);
        readThread.start();
    }

    @Override
    public void subscribe() {
        this.connectionType = "SUBSCRIBER";
        connectAndRegister();

        Thread readThread = new Thread(this);
        readThread.setDaemon(true);
        readThread.start();
    }

    @Override public void sendMessage(Message m) { sendQueue.offer(m); }
    @Override public Message receiveMessageFromQueue() throws InterruptedException { return receiveQueue.take(); }
    @Override public void setServiceType(ServicesTypes s) { this.serviceType = s; }
    @Override public void setSectorId(String s) { this.sectorId = s; }
}
