package io.nexus.gateway.opcua;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Minimal OPC-UA operations needed by Connector. Isolates Milo from test code. */
public interface OpcUaClientFacade {

    void connect() throws Exception;

    /** Subscribe to monitored items; callback fires on each value change. */
    void subscribe(List<String> nodeIds, BiConsumer<String, OpcValue> onValue) throws Exception;

    /** One-shot read of the given node IDs. */
    Map<String, OpcValue> read(List<String> nodeIds) throws Exception;

    /** Browse children of rootNodeId; returns map of nodeId → display name. */
    Map<String, String> browse(String rootNodeId) throws Exception;

    void close() throws Exception;
}
