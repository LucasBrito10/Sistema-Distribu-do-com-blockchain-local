package com.ormuz.shared.enums;

public enum TopicType {
    
    SECURITY_ALERTS("Alerta Crítico e Segurança"),
    MARITIME_TRAFFIC("Dados de Navegação e Tráfego"),
    ENVIRONMENTAL_TELEMETRY("Leituras de Sensores e Clima"),
    MULTIMEDIA_STREAM("Áudio, Vídeo e Imagens"),
    MAINTENANCE_LOGS("Inspeção e Status de Hardware"),
    CONTROL_COMMANDS("Instruções de Controle e Atuadores");

    private final String description;

    TopicType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}