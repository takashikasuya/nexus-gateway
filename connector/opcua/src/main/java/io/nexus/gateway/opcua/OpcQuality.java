package io.nexus.gateway.opcua;

public enum OpcQuality {
    GOOD, UNCERTAIN, BAD;

    public String toCommonQuality() {
        return switch (this) {
            case GOOD -> "Good";
            case UNCERTAIN -> "Uncertain";
            case BAD -> "Bad";
        };
    }
}
