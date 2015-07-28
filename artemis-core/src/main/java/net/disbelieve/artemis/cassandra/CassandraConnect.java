package com.comcast.artemis.cassandra;

/**
 * Created by kmatth002c on 12/10/2014.
 */


import com.comcast.artemis.exception.ConnectionException;
import com.comcast.artemis.jmx.MXBeansManager;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

/**
 * The type Cassandra connect.
 */
public class CassandraConnect {
    private static Cluster cluster;
    private static Session session;
    private final String[] contactPoints;
    private final Integer port;
    private final String compression;
    private final String localDataCenter;
    private final String userName;
    private final String password;
    private final Integer coreConnectionsPerHost;
    private final Integer idleTimeoutSeconds;
    private final Integer maxConnectionsPerHost;
    private final Integer maxSimultaneousRequestsPerConnectionThreshold;
    private final Integer poolTimeoutMillis;
    private static final Object CLUSTER_LOCK = new Object();

    public static enum PROPERTIES {
        CONTACT_POINTS("cassandra.contactPoints"),
        PORT("cassandra.port"),
        COMPRESSION("cassandra.compression"),
        LOCAL_DATA_CENTER("cassandra.localDataCenter"),
        WRITE_CONSISTENCY_LEVEL("cassandra.writeConsistency"),
        USER_NAME("cassandra.userName"),
        PASSWORD("cassandra.password"),
        READ_CONSISTENCY_LEVEL("cassandra.readConsistency"),
        CORE_CONNECTIONS_PER_HOST("cassandra.coreConnectionsPerHost"),
        IDLE_TIMEOUT("cassandra.idleTimeout"),
        MAX_CONNECTIONS_PER_HOST("cassandra.maxConnectionsPerHost"),
        MAX_REQUESTS_PER_CONNECTION("cassandra.maxRequestsPerConnection"),
        POOL_TIMEOUT("cassandra.poolTimeout");

        private final String s;

        PROPERTIES(String s) {
            this.s = s;
        }

        public String toString() {
            return s;
        }
    }

    private CassandraConnect(ConnectionBuilder builder) {
        this.contactPoints = builder.contactPoints.replaceAll(" ", "").split(",");
        this.port = builder.port;
        this.compression = builder.compression;
        this.localDataCenter = builder.localDataCenter;
        this.userName = builder.userName;
        this.password = builder.password;
        this.coreConnectionsPerHost = builder.coreConnectionsPerHost;
        this.idleTimeoutSeconds = builder.idleTimeoutSeconds;
        this.maxConnectionsPerHost = builder.maxConnectionsPerHost;
        this.maxSimultaneousRequestsPerConnectionThreshold = builder.maxSimultaneousRequestsPerConnectionThreshold;
        this.poolTimeoutMillis = builder.poolTimeoutMillis;
    }

    /**
     * Connect to the Cassandra cluster.
     */
    public void connect() throws ConnectionException {
        synchronized (CLUSTER_LOCK) {
            if (cluster != null) {
                throw new ConnectionException("Connection already exists!");
            }
            Cluster.Builder clusterBuilder = Cluster.builder();
            PoolingOptions poolingOptions = new PoolingOptions();

            if (maxConnectionsPerHost != null) {
                poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost);
            }
            if (coreConnectionsPerHost != null) {
                poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsPerHost);
            }
            if (maxSimultaneousRequestsPerConnectionThreshold != null) {
                poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, maxSimultaneousRequestsPerConnectionThreshold);
            }
            if (idleTimeoutSeconds != null) {
                poolingOptions.setIdleTimeoutSeconds(idleTimeoutSeconds);
            }
            if (poolTimeoutMillis != null) {
                poolingOptions.setPoolTimeoutMillis(poolTimeoutMillis);
            }

            if (localDataCenter != null) {
                DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy = new DCAwareRoundRobinPolicy(localDataCenter);
                clusterBuilder = clusterBuilder.withLoadBalancingPolicy(dcAwareRoundRobinPolicy);
            }
            if (userName != null) {
                clusterBuilder = clusterBuilder.withCredentials(userName, password);
            }
            if (port != null) {
                clusterBuilder.withPort(port);
            }
            if (compression != null) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.valueOf(compression));
            }
            clusterBuilder.withPoolingOptions(poolingOptions);
            //NOSONAR intentional assignment of static var in non-static method protected by lock.
            clusterBuilder.addContactPoints(contactPoints);
            cluster = clusterBuilder.build();
            //NOSONAR intentional assignment of static var in non-static method protected by lock.
            session = cluster.connect();
            MXBeansManager.registerCassandraMetadata(session.getCluster());
        }
    }

    /**
     * Close the connection to the cluster
     */
    public void close() {
        cluster.close();
    }

    /**
     * Gets a session to execute cql against this cluster.
     *
     * @return the session for this connection
     */
    public static Session getSession() {
        return session;
    }

    public static class ConnectionBuilder {
        private String contactPoints;
        private Integer port;
        private String compression;
        private String localDataCenter;
        private String userName;
        private String password;
        private Integer coreConnectionsPerHost;
        private Integer idleTimeoutSeconds;
        private Integer maxConnectionsPerHost;
        private Integer maxSimultaneousRequestsPerConnectionThreshold;
        private Integer poolTimeoutMillis;

        public ConnectionBuilder contactPoints(String contactPoint) {
            this.contactPoints = contactPoint;
            return this;
        }

        public ConnectionBuilder port(int port) {
            this.port = port;
            return this;
        }

        public ConnectionBuilder compression(String compression) {
            this.compression = compression;
            return this;
        }

        public ConnectionBuilder localDataCenter(String localDataCenter) {
            this.localDataCenter = localDataCenter;
            return this;
        }

        public ConnectionBuilder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public ConnectionBuilder password(String password) {
            this.password = password;
            return this;
        }

        public ConnectionBuilder coreConnectionsPerHost(Integer coreConnectionsPerHost) {
            this.coreConnectionsPerHost = coreConnectionsPerHost;
            return this;
        }

        public ConnectionBuilder idleTimeoutSeconds(Integer idleTimeoutSeconds) {
            this.idleTimeoutSeconds = idleTimeoutSeconds;
            return this;
        }

        public ConnectionBuilder maxConnectionsPerHost(Integer maxConnectionsPerHost) {
            this.maxConnectionsPerHost = maxConnectionsPerHost;
            return this;
        }

        public ConnectionBuilder maxSimultaneousRequestsPerConnectionThreshold(Integer maxSimultaneousRequestsPerConnectionThreshold) {
            this.maxSimultaneousRequestsPerConnectionThreshold = maxSimultaneousRequestsPerConnectionThreshold;
            return this;
        }

        public ConnectionBuilder poolTimeoutMillis(Integer poolTimeoutMillis) {
            this.poolTimeoutMillis = poolTimeoutMillis;
            return this;
        }

        public CassandraConnect build() {
            return new CassandraConnect(this);
        }
    }
}
