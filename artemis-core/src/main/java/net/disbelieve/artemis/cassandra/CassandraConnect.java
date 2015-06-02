package net.disbelieve.artemis.cassandra;

/**
 * Created by kmatth002c on 12/10/2014.
 */


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static enum PROPERTIES {
        CONTACT_POINTS("cassandra.contactPoints"),
        PORT("cassandra.port"),
        COMPRESSION("cassandra.compression"),
        LOCAL_DATA_CENTER("cassandra.localDataCenter"),
        WRITE_CONSISTENCY_LEVEL("cassandra.writeConsistency"),
        USER_NAME("cassandra.userName"),
        PASSWORD("cassandra.password"),
        READ_CONSISTENCY_LEVEL("cassandra.readConsistency");

        private final String s;

        public String toString() {
            return s;
        }

        PROPERTIES(String s) {
            this.s = s;
        }

    }

    /**
     * The Logger.
     */
    Logger logger = LoggerFactory.getLogger(CassandraConnect.class);

    private CassandraConnect(ConnectionBuilder builder) {
        this.contactPoints = builder.contactPoints.split(",");
        this.port = builder.port;
        this.compression = builder.compression;
        this.localDataCenter = builder.localDataCenter;
        this.userName = builder.userName;
        this.password = builder.password;
    }

    /**
     * Connect to the Cassandra cluster.
     */
    public void connect() {
        Cluster.Builder clusterBuilder = Cluster.builder();

        /* if (... some pooling options ) {
         *     PoolingOptions poolingOptions = new PoolingOptions();
         *     poolingOptions.setXXX ...
         *     clusterBuilder.withPoolingOptions(poolingOptions)
         */

        if (localDataCenter != null) {
            DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy = new DCAwareRoundRobinPolicy(localDataCenter);
            clusterBuilder = clusterBuilder.withLoadBalancingPolicy(dcAwareRoundRobinPolicy);

        }
        if (userName != null) {
            clusterBuilder = clusterBuilder.withCredentials(userName, password);
        }
        if(port != null) {
                clusterBuilder.withPort(port);
        }
        cluster = clusterBuilder
                .addContactPoints(contactPoints)
                .withCompression(ProtocolOptions.Compression.valueOf(compression))
                .build();
        session = cluster.connect();
    }

    /**
     * Close the connection to the cluster
     */
    public static void close() {
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

        public CassandraConnect build() {
            return new CassandraConnect(this);
        }
    }
}
