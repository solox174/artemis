package net.disbelieve.wicca.jersey.filter;

import net.disbelieve.wicca.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.wicca.jmx.MXBeansManager;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;

/**
 * Created by kmatth002c on 1/6/2015.
 */
public class CassandraConsistencyLevelFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        CassandraMetadataMXBeanImpl cassandraMetadata = new CassandraMetadataMXBeanImpl();
        String cassandraConsistencyLevel;

        if ((cassandraConsistencyLevel = containerRequestContext.getHeaderString("CASSANDRA_WRITE_CONSISTENCY")) != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL_OVERRIDE.toString(),
                    cassandraConsistencyLevel);
        } else {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL_OVERRIDE.toString(), null);
        }
        if ((cassandraConsistencyLevel = containerRequestContext.getHeaderString("CASSANDRA_READ_CONSISTENCY")) != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL_OVERRIDE.toString(),
                    cassandraConsistencyLevel);
        } else {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL_OVERRIDE.toString(), null);
        }
    }
}
