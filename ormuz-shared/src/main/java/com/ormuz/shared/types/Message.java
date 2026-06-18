package com.ormuz.shared.types;

import java.io.Serializable;
import com.ormuz.shared.enums.CommandType;
import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.enums.TopicType;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private ServicesTypes serviceType;
    private String connectionType;
    private String nodeId;
    private int data = -1;
    private CommandType commandType;
    private String targetNodeId;
    private String sectorId;
    private java.util.List<ServicesTypes> dataTypes;
    private String tokenId;
    private long requestTimestamp;
    private String requestSignature;
    private String missionId;
    private String routeId;
    private String missionReport;

    public Message() {}

    public TopicType getTopicType() {
        return serviceType != null ? serviceType.getDefaultTopic() : null;
    }
    public void setTopicType(TopicType topicType) {}

    public ServicesTypes getServiceType() { return serviceType; }
    public void setServiceType(ServicesTypes s) { this.serviceType = s; }

    public String getConnectionType() { return connectionType; }
    public void setConnectionType(String c) { this.connectionType = c; }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String n) { this.nodeId = n; }

    public int getData() { return data; }
    public void setData(int d) { this.data = d; }

    public CommandType getCommandType() { return commandType; }
    public void setCommandType(CommandType c) { this.commandType = c; }

    public String getTargetNodeId() { return targetNodeId; }
    public void setTargetNodeId(String t) { this.targetNodeId = t; }

    public String getSectorId() { return sectorId; }
    public void setSectorId(String s) { this.sectorId = s; }

    public java.util.List<ServicesTypes> getDataTypes() { return dataTypes; }
    public void setDataTypes(java.util.List<ServicesTypes> d) { this.dataTypes = d; }

    public String getTokenId() { return tokenId; }
    public void setTokenId(String t) { this.tokenId = t; }

    public long getRequestTimestamp() { return requestTimestamp; }
    public void setRequestTimestamp(long requestTimestamp) { this.requestTimestamp = requestTimestamp; }

    public String getRequestSignature() { return requestSignature; }
    public void setRequestSignature(String requestSignature) { this.requestSignature = requestSignature; }

    public String getMissionId() { return missionId; }
    public void setMissionId(String missionId) { this.missionId = missionId; }

    public String getRouteId() { return routeId; }
    public void setRouteId(String routeId) { this.routeId = routeId; }

    public String getMissionReport() { return missionReport; }
    public void setMissionReport(String m) { this.missionReport = m; }

    @Override
    public String toString() {
        return "Message{service=" + serviceType +
               ", node='" + nodeId + '\'' +
               ", sector='" + sectorId + '\'' +
               ", data=" + data +
               ", cmd=" + commandType +
               ", target='" + targetNodeId + '\'' +
               ", token='" + tokenId + '\'' +
               ", mission='" + missionId + '\'' +
               ", route='" + routeId + '\'' +
               ", conn='" + connectionType + '\'' + '}';
    }
}