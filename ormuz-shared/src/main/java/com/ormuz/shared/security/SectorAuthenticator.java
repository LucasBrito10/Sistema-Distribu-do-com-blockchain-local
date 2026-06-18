package com.ormuz.shared.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.ormuz.shared.enums.ServicesTypes;
import com.ormuz.shared.types.Message;

/**
 * Autenticação de mensagens críticas do consórcio ORMUZ.
 *
 * <p>As requisições comuns de sensores/drones usam ORMUZ_AUTH_SECRET. Operações
 * administrativas, como emissão/recarga de tokens, usam ORMUZ_ADMIN_SECRET.
 * Assim, um cliente comum autenticado não consegue emitir saldo apenas enviando
 * ADD_TOKENS.</p>
 */
public final class SectorAuthenticator {
    private static final String DEFAULT_SECRET = "ORMUZ_CONSORTIUM_SECRET_DEMO";
    private static final String DEFAULT_ADMIN_SECRET = "ORMUZ_ADMIN_SECRET_DEMO";

    private SectorAuthenticator() {}

    /** Assina mensagem comum de sensor, drone ou broker interno. */
    public static void sign(Message message) {
        signWithSecret(message, secret());
    }

    /** Assina mensagem administrativa, usada para recarga/emissão de tokens. */
    public static void signAdmin(Message message) {
        signWithSecret(message, adminSecret());
    }

    private static void signWithSecret(Message message, String signingSecret) {
        if (message == null) return;
        ensureReplayFields(message);
        message.setRequestSignature(calculateSignature(message, signingSecret));
    }

    private static void ensureReplayFields(Message message) {
        if (message.getRequestTimestamp() <= 0) {
            message.setRequestTimestamp(System.currentTimeMillis());
        }
        if (message.getTokenId() == null || message.getTokenId().isBlank()) {
            String node = message.getNodeId() != null ? message.getNodeId() : "NODE";
            message.setTokenId("REQ-" + node + "-" + message.getRequestTimestamp() + "-" + Math.abs(message.hashCode()));
        }
    }

    /** Verifica mensagem comum de sensor, drone ou broker interno. */
    public static boolean verify(Message message) {
        return verifyWithSecret(message, secret());
    }

    /** Verifica mensagem administrativa, como ADD_TOKENS. */
    public static boolean verifyAdmin(Message message) {
        return verifyWithSecret(message, adminSecret());
    }

    private static boolean verifyWithSecret(Message message, String signingSecret) {
        if (message == null) return false;
        if (message.getRequestTimestamp() <= 0) return false;
        if (message.getRequestSignature() == null || message.getRequestSignature().isBlank()) return false;
        return constantTimeEquals(message.getRequestSignature(), calculateSignature(message, signingSecret));
    }

    public static String calculateSignature(Message message) {
        return calculateSignature(message, secret());
    }

    private static String calculateSignature(Message message, String signingSecret) {
        String canonical = canonical(message) + "|secret=" + signingSecret;
        return sha256Hex(canonical);
    }

    /**
     * Forma canônica assinada. Inclui todos os campos que alteram semântica,
     * roteamento, pagamento e laudo de missão.
     */
    private static String canonical(Message m) {
        return "sector=" + nullSafe(m.getSectorId())
                + "|node=" + nullSafe(m.getNodeId())
                + "|token=" + nullSafe(m.getTokenId())
                + "|connection=" + nullSafe(m.getConnectionType())
                + "|command=" + (m.getCommandType() != null ? m.getCommandType().name() : "")
                + "|target=" + nullSafe(m.getTargetNodeId())
                + "|service=" + (m.getServiceType() != null ? m.getServiceType().name() : "")
                + "|data=" + m.getData()
                + "|dataTypes=" + canonicalDataTypes(m)
                + "|missionId=" + nullSafe(m.getMissionId())
                + "|routeId=" + nullSafe(m.getRouteId())
                + "|missionReport=" + nullSafe(m.getMissionReport())
                + "|ts=" + m.getRequestTimestamp();
    }

    private static String canonicalDataTypes(Message m) {
        if (m.getDataTypes() == null || m.getDataTypes().isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (ServicesTypes type : m.getDataTypes()) {
            if (sb.length() > 0) sb.append(',');
            sb.append(type != null ? type.name() : "");
        }
        return sb.toString();
    }

    private static String secret() {
        String env = System.getenv("ORMUZ_AUTH_SECRET");
        return env == null || env.isBlank() ? DEFAULT_SECRET : env;
    }

    private static String adminSecret() {
        String env = System.getenv("ORMUZ_ADMIN_SECRET");
        return env == null || env.isBlank() ? DEFAULT_ADMIN_SECRET : env;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        int max = Math.max(a.length(), b.length());
        int result = a.length() ^ b.length();
        for (int i = 0; i < max; i++) {
            char ca = i < a.length() ? a.charAt(i) : 0;
            char cb = i < b.length() ? b.charAt(i) : 0;
            result |= ca ^ cb;
        }
        return result == 0;
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hashBytes.length * 2);
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) sb.append('0');
                sb.append(hex);
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }
}
