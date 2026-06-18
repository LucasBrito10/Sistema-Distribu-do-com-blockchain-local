package com.ormuz.shared.enums;

public enum CommandType {

    // Ciclo de Vida
    ACTIVATE("Ativar Recurso/Serviço"),
    DEACTIVATE("Desativar Recurso/Serviço"),
    RESTART("Reiniciar Módulo"),
    REQUEST_STATUS("Solicitar Relatório de Status"),

    // Movimentação e Rastreio
    MOVE_TO_COORDINATE("Deslocar para Coordenada Específica"),
    RETURN_TO_BASE("Retornar à Base/Ponto de Origem"),
    TRACK_TARGET("Iniciar Rastreio Contínuo de Alvo"),

    // Controle de Dados e Sensores
    START_DATA_STREAM("Iniciar Transmissão de Dados Contínua"),
    STOP_DATA_STREAM("Interromper Transmissão de Dados"),
    CALIBRATE_SENSOR("Executar Calibração de Sensores"),
    SET_DATA_TYPES("Configurar Tipos de Dados Ativos no Sensor"),

    // Orquestração de Recursos
    REASSIGN_SECTOR("Reassociar Recurso a Outro Setor"),
    REASSIGN_MISSION("Reatribuir missão já paga sem novo débito"),

    // Emergência
    ENGAGE_SAR_MODE("Acionar Protocolo de Busca e Resgate (SAR)"),
    TRIGGER_ALARM("Acionar Alarmes Físicos/Visuais"),

    // Operações Financeiras e Blockchain
    PAYMENT_REJECTED("Pagamento de token rejeitado"),
    ADD_TOKENS("Recarregar saldo de tokens do setor");

    private final String description;

    CommandType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}