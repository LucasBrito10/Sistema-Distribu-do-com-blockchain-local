package com.ormuz.client.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import java.util.UUID;

import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.security.SectorAuthenticator;
import com.ormuz.shared.types.Message;
import com.ormuz.shared.util.ConsoleLog;

public class Client {
    private final Map<ServicesTypes, MiddlewareClientInterface> publishers  = new HashMap<>();
    private final Map<ServicesTypes, MiddlewareClientInterface> subscribers = new HashMap<>();
    // Histórico circular dos últimos 10 valores por tópico
    private final Map<ServicesTypes, LinkedList<Integer>> topicHistory = new HashMap<>();

    private String id;
    private String sectorId;

    private volatile Message responseMessage = null;
    private final Object syncLock = new Object();

    public final void setId(String id)        { this.id = id; }
    public final void setSectorId(String s)   { this.sectorId = s; }
    public final String getId()               { return id; }
    public final String getSectorId()         { return sectorId; }

    public boolean startPublisher(String host, int port, ServicesTypes s) {
        try {
            MiddlewareClientInterface p = new MiddlewareClientBuilder()
                    .withHost(host).withPort(port).withId(id).build();
            p.setServiceType(s);
            p.setSectorId(sectorId);
            p.publish();
            publishers.put(s, p);
            startListening(p, s);
            return true;
        } catch (IOException e) {
            ConsoleLog.error("CLIENT", "PUBLISHER_FALHOU", "cliente", id, "serviço", s.name(), "erro", e.getMessage());
            return false;
        }
    }

    public boolean startSubscribe(String host, int port, ServicesTypes s) {
        try {
            MiddlewareClientInterface sub = new MiddlewareClientBuilder()
                    .withHost(host).withPort(port).withId(id).build();
            sub.setServiceType(s);
            sub.setSectorId(sectorId);
            sub.subscribe();
            subscribers.put(s, sub);
            topicHistory.put(s, new LinkedList<>());
            startListening(sub, s);
            return true;
        } catch (IOException e) {
            ConsoleLog.error("CLIENT", "SUBSCRIBER_FALHOU", "cliente", id, "serviço", s.name(), "erro", e.getMessage());
            return false;
        }
    }

    private void startListening(MiddlewareClientInterface m, ServicesTypes s) {
        Thread t = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Message msg = m.receiveMessageFromQueue();

                    // Mensagem direcionada a este nó (ex: comando ACTIVATE do broker)
                    if (msg.getTargetNodeId() != null && msg.getTargetNodeId().equals(id)) {
                        synchronized (syncLock) {
                            responseMessage = msg;
                            syncLock.notifyAll();
                        }
                    }
                    // Mensagem de dados normais: atualiza histórico
                    if (msg.getData() != -1) {
                        // BUG CORRIGIDO: sincronização unificada em syncLock (era `this`)
                        synchronized (syncLock) {
                            LinkedList<Integer> h = topicHistory.get(msg.getServiceType());
                            if (h != null) {
                                if (h.size() >= 10) h.removeFirst();
                                h.addLast(msg.getData());
                            }
                        }
                    }

                    handleIncomingMessage(msg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.setDaemon(true);
        t.start();
    }

    /** Hook para subclasses reagirem a mensagens recebidas. */
    protected void handleIncomingMessage(Message m) {}

    /** Envia dados para um serviço via publisher. */
    public void sendMessage(int d, ServicesTypes s) {
        MiddlewareClientInterface p = publishers.get(s);
        if (p == null) {
            ConsoleLog.warn("CLIENT", "PUBLISHER_AUSENTE", "cliente", id, "serviço", s.name());
            return;
        }
        Message m = new Message();
        m.setData(d);
        m.setNodeId(id);
        m.setSectorId(sectorId);
        m.setConnectionType("PUBLISHER");
        m.setServiceType(s);
        m.setTokenId("REQ-" + id + "-" + UUID.randomUUID());
        prepareMissionMetadataIfEscort(m);
        SectorAuthenticator.sign(m);
        p.sendMessage(m);
    }

    /** Envia uma Message já montada via qualquer publisher disponível (ex: comandos de controle). */
    public void sendCommandMessage(Message m) {
        MiddlewareClientInterface p = publishers.isEmpty() ? null : publishers.values().iterator().next();
        if (p == null) {
            ConsoleLog.warn("CLIENT", "COMANDO_NÃO_ENVIADO", "cliente", id, "motivo", "nenhum publisher disponível");
            return;
        }
        if (m.getNodeId() == null) m.setNodeId(id);
        if (m.getSectorId() == null) m.setSectorId(sectorId);
        if (m.getTokenId() == null || m.getTokenId().isBlank()) m.setTokenId("CMD-" + id + "-" + UUID.randomUUID());
        prepareMissionMetadataIfEscort(m);
        if (m.getRequestSignature() == null || m.getRequestSignature().isBlank()) {
            SectorAuthenticator.sign(m);
        }
        p.sendMessage(m);
    }


    private void prepareMissionMetadataIfEscort(Message m) {
        if (m == null || m.getData() != 1 || m.getServiceType() == null) return;
        if (m.getServiceType() != ServicesTypes.INTRUSION_DETECTION
                && m.getServiceType() != ServicesTypes.SEARCH_AND_RESCUE) {
            return;
        }
        String safeSector = m.getSectorId() != null ? m.getSectorId() : (sectorId != null ? sectorId : "SETOR_DESCONHECIDO");
        String safeNode = m.getNodeId() != null ? m.getNodeId() : (id != null ? id : "NODE");
        if (m.getMissionId() == null || m.getMissionId().isBlank()) {
            m.setMissionId("MIS-" + safeNode + "-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8));
        }
        if (m.getRouteId() == null || m.getRouteId().isBlank()) {
            m.setRouteId("ROTA-" + safeSector + "-" + safeNode);
        }
    }

    /** Envia uma resposta direcionada a um alvo específico (ex: sinal de conclusão do drone). */
    public void sendResponse(int d, ServicesTypes s, String targetId) {
        
        MiddlewareClientInterface p = publishers.get(s);
        if (p == null) {
            ConsoleLog.warn("CLIENT", "RESPOSTA_NÃO_ENVIADA", "cliente", id, "serviço", s.name(), "motivo", "publisher ausente");
            return;
        }
        Message msg = new Message();
        msg.setData(d);
        msg.setNodeId(id);
        msg.setSectorId(sectorId);
        msg.setServiceType(s);
        msg.setTargetNodeId(targetId);
        msg.setTokenId("RESP-" + id + "-" + UUID.randomUUID());
        prepareMissionMetadataIfEscort(msg);
        SectorAuthenticator.sign(msg);
        p.sendMessage(msg);
    }

    
    public Message waitForResponse(long timeoutMs) throws InterruptedException {
        synchronized (syncLock) {
            if (responseMessage != null) {
                Message r = responseMessage;
                responseMessage = null;
                return r;
            }
            syncLock.wait(timeoutMs);
            Message r = responseMessage;
            responseMessage = null;
            return r;
        }
    }

    /** @deprecated Use waitForResponse() — este método não reseta o estado. */
    @Deprecated
    public Message getResponseMessage() {
        return responseMessage;
    }
}
