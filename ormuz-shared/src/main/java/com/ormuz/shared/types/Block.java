package com.ormuz.shared.types;

import java.io.Serializable;

/**
 * Um bloco da blockchain distribuída do sistema ORMUZ.
 * Cada bloco registra uma transação de token de forma imutável.
 * O campo {@code hash} é o SHA-256 canônico dos campos do bloco e da transação.
 */
public class Block implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Posição global do bloco na cadeia distribuída (0 = genesis). */
    private int index;
    /** Timestamp em milissegundos da criação do bloco. */
    private long timestamp;
    /** Transação que este bloco registra. */
    private Transaction transaction;
    /** Hash SHA-256 do bloco anterior (garante imutabilidade da cadeia). */
    private String previousHash;
    /** Hash SHA-256 deste bloco, calculado em {@link com.ormuz.shared.blockchain.Blockchain}. */
    private String hash;

    public Block() {}

    public Block(int index, Transaction transaction, String previousHash) {
        this.index        = index;
        this.timestamp    = System.currentTimeMillis();
        this.transaction  = transaction;
        this.previousHash = previousHash;
    }

    public int          getIndex()              { return index; }
    public void         setIndex(int v)         { this.index = v; }

    public long         getTimestamp()          { return timestamp; }
    public void         setTimestamp(long v)    { this.timestamp = v; }

    public Transaction  getTransaction()        { return transaction; }
    public void         setTransaction(Transaction v) { this.transaction = v; }

    public String       getPreviousHash()       { return previousHash; }
    public void         setPreviousHash(String v){ this.previousHash = v; }

    public String       getHash()               { return hash; }
    public void         setHash(String v)       { this.hash = v; }

    @Override
    public String toString() {
        String h = (hash         != null && hash.length()         >= 8) ? hash.substring(0, 8)         + "..." : hash;
        String p = (previousHash != null && previousHash.length() >= 8) ? previousHash.substring(0, 8) + "..." : previousHash;
        return "Block{#" + index + ", hash=" + h + ", prev=" + p + ", tx=" + transaction + "}";
    }
}
