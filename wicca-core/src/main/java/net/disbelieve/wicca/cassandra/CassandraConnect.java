package net.disbelieve.wicca.cassandra;

/**
 * Created by kmatth002c on 12/10/2014.
 */


import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Cassandra connect.
 */
public class CassandraConnect {
    private static Cluster cluster;
    private static Session session;
    /**
     * The Logger.
     */
    Logger logger = LoggerFactory.getLogger(CassandraConnect.class);

    /**
     * Connect to the Cassandra cluster.
     *
     * @param node the IP address of a seed node for the cluster
     */
    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withCompression(ProtocolOptions.Compression.LZ4)
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
}