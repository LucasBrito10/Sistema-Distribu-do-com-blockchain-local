package com.ormuz.shared.types;

import java.io.Serializable;
import com.ormuz.shared.enums.TopicType;

public class ClientData implements Serializable {
    private static final long serialVersionUID = 1L;
    private String nodeId;
    private String sectorId;
    private TopicType topic;
    private String connectionType;
    private String connectedBrokerId;

    public ClientData() {}

    public ClientData(String nodeId, String sectorId, String connectionType,
                      String connectedBrokerId, TopicType topic) {
        this.nodeId = nodeId;
        this.sectorId = sectorId;
        this.connectionType = connectionType;
        this.connectedBrokerId = connectedBrokerId;
        this.topic = topic;
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public String getSectorId() { return sectorId; }
    public void setSectorId(String sectorId) { this.sectorId = sectorId; }
    public String getConnectionType() { return connectionType; }
    public void setConnectionType(String connectionType) { this.connectionType = connectionType; }
    public String getConnectedBrokerId() { return connectedBrokerId; }
    public void setConnectedBrokerId(String connectedBrokerId) { this.connectedBrokerId = connectedBrokerId; }
    public TopicType getTopic() { return topic; }
    public void setTopic(TopicType topic) { this.topic = topic; }

    @Override
    public String toString() {
        return "ClientData{id='" + nodeId + "', sector='" + sectorId +
               "', type='" + connectionType + "', broker='" + connectedBrokerId + "'}";
    }
}
