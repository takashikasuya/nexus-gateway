package io.nexus.gateway.opcua;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        log.info("opcua: starting connector={} endpoint={}", cfg.connectorId(), cfg.opcuaEndpoint());

        Options natsOpts = new Options.Builder()
            .server(cfg.natsUrl())
            .connectionListener((conn, type) -> log.info("nats: {}", type))
            .errorListener(new io.nats.client.ErrorListener() {})
            .build();

        Connection nats = Nats.connect(natsOpts);
        var js = nats.jetStream();

        Connector.Publisher publisher = (subject, data) -> js.publish(subject, data);

        MiloOpcUaClientFacade miloClient = new MiloOpcUaClientFacade(cfg.opcuaEndpoint());
        Connector connector = new Connector(cfg, miloClient, publisher);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("opcua: shutdown signal received");
            connector.stop();
        }));

        connector.run();
        nats.close();
    }
}
