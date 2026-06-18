package com.ormuz.broker.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ormuz.broker.core.Broker;
import com.ormuz.broker.log.BrokerLogger;
import com.ormuz.broker.log.BrokerLogger.LogType;
import com.ormuz.shared.enums.TopicType;
import com.ormuz.shared.types.ClientData;
import com.ormuz.shared.types.Message;

class MiddlewareHandlerServer implements MiddlewareHandlerInterface {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Socket clientSocket;
    private String nodeId;
    private final Broker broker;
    private String connectionType;
    private TopicType topic;
    private final BufferedReader in;
    private final PrintWriter out;

    MiddlewareHandlerServer(Socket socket, Broker broker) {
        this.clientSocket = socket;
        this.broker = broker;
        try {
            this.in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            throw new RuntimeException("Falha ao abrir streams do socket", e);
        }
    }

    @Override
    public void sendMessage(Message message) {
        try {
            String json = MAPPER.writeValueAsString(message);
            this.out.println(json);
        } catch (IOException e) {
            BrokerLogger.err(LogType.MENSAGEM, "[HANDLER] Erro ao serializar mensagem para " + nodeId + ": " + e.getMessage());
        }
    }

    private Message receiveMessage() {
        try {
            String json = this.in.readLine();
            if (json == null) return null;
            return MAPPER.readValue(json, Message.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void run() {
        String topicKey = null;
        try {
            Message init = receiveMessage();
            if (init == null) {
                BrokerLogger.err(LogType.CONEXAO, "[HANDLER] Conexão encerrada antes do registro.");
                return;
            }

            // BUG CORRIGIDO: guard para serviceType null no registro inicial
            if (init.getServiceType() == null) {
                BrokerLogger.err(LogType.MENSAGEM, "[HANDLER] Mensagem de registro sem serviceType. Descartando.");
                return;
            }

            this.nodeId = init.getNodeId();
            if (this.nodeId == null || this.nodeId.isBlank()) {
                BrokerLogger.err(LogType.MENSAGEM, "[HANDLER] Mensagem de registro sem nodeId. Descartando.");
                return;
            }
            this.topic  = init.getServiceType().getDefaultTopic();
            this.connectionType = init.getConnectionType();
            topicKey = init.getSectorId() + ":" + this.topic;

            BrokerLogger.log(LogType.CONEXAO, "[HANDLER] Registrando nó: " + nodeId + " | tipo: " + connectionType
                               + " | tópico: " + topic + " | setor: " + init.getSectorId());

            this.broker.addConnection(this.topic, this.nodeId, this);

            ClientData clientData = new ClientData(
                this.nodeId, init.getSectorId(), this.connectionType,
                this.broker.getBrokerId(), this.topic
            );
            this.broker.getSharedClients().put(this.nodeId, clientData);

            if ("SUBSCRIBER".equals(this.connectionType)) {
                this.broker.getSharedTopics().put(topicKey, this.nodeId);
                BrokerLogger.log(LogType.CONEXAO, "[HANDLER] Subscriber " + nodeId + " inscrito em: " + topicKey);
            }

            Message incoming;
            while ((incoming = receiveMessage()) != null) {
                this.broker.processMessage(incoming);
            }

        } catch (Exception e) {
            BrokerLogger.err(LogType.CONEXAO, "[HANDLER] Exceção no handler de " + this.nodeId + ": " + e.getMessage());
        } finally {
            this.broker.removeConnection(this.topic, this.nodeId, this);
            if (this.nodeId != null && topicKey != null && "SUBSCRIBER".equals(this.connectionType)) {
                this.broker.getSharedTopics().remove(topicKey, this.nodeId);
            }
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }
}
