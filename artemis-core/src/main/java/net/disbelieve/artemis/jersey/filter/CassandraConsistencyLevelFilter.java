package net.disbelieve.artemis.jersey.filter;

import net.disbelieve.artemis.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.artemis.jmx.MXBeansManager;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;

/**
 * Created by kmatth002c on 1/6/2015.
 */
public class CassandraConsistencyLevelFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        String cassandraConsistencyLevel;

        if ((cassandraConsistencyLevel = containerRequestContext.getHeaderString("CASSANDRA_WRITE_CONSISTENCY")) != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL_OVERRIDE.toString(),
                    cassandraConsistencyLevel);
        } else {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL_OVERRIDE.toString(),
                    MXBeansManager.getMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                            CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL.toString()).toString());
        }
        if ((cassandraConsistencyLevel = containerRequestContext.getHeaderString("CASSANDRA_READ_CONSISTENCY")) != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL_OVERRIDE.toString(),
                    cassandraConsistencyLevel);
        } else {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL_OVERRIDE.toString(),
                    MXBeansManager.getMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                            CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL.toString()).toString());
        }
    }
}
