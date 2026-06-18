package com.ormuz.shared.enums;

/**
 * Tipos de transação aceitos pelo ledger distribuído ORMUZ.
 * O saldo dos setores é sempre derivado da sequência desses eventos.
 */
public enum TransactionType {
    GENESIS,
    ISSUE,
    CREDIT,
    PAYMENT,
    PAYMENT_REJECTED,
    MISSION_LOG
}
