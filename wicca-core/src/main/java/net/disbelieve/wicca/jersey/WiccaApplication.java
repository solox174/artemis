package net.disbelieve.wicca.jersey;

import net.disbelieve.wicca.cassandra.CassandraConnect;
import net.disbelieve.wicca.jersey.filter.CassandraConsistencyLevelFilter;
import net.disbelieve.wicca.jmx.MXBeansManager;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;

/**
 * Created by kmatth002c on 12/10/2014.
 */

public
@ApplicationPath("/*")
class WiccaApplication extends ResourceConfig {
    private CassandraConnect cassandra;

    public WiccaApplication() {
        cassandra = new CassandraConnect();
        cassandra.connect("127.0.0.1");
        MXBeansManager.registerCassandraMetadata(cassandra.getSession().getCluster());
        packages("net.disbelieve.wicca.jersey");
        register(JacksonFeature.class);
        register(CassandraConsistencyLevelFilter.class);
    }
}
