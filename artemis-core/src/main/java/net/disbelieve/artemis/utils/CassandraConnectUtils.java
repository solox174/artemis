package com.comcast.artemis.utils;

import com.comcast.artemis.cassandra.CassandraConnect;
import com.comcast.artemis.ecryption.EncryptionUtils;
import com.comcast.artemis.jmx.CassandraMetadataMXBeanImpl;
import com.comcast.artemis.jmx.MXBeansManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by kmatth207 on 6/10/15.
 */
public class CassandraConnectUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraConnectUtils.class);
    private String contactPoints;
    private Integer port;
    private String compression;
    private String localDataCenter;
    private String userName;
    private String password;
    private Integer coreConnectionsPerHost;
    private Integer idleTimeout;
    private Integer maxConnectionsPerHost;
    private Integer maxRequestsPerConnection;
    private Integer poolTimeout;

    private CassandraConnectUtils() {}

    public CassandraConnectUtils(Properties properties) {
        try {
            String prop;

            contactPoints = properties.getProperty(CassandraConnect.PROPERTIES.CONTACT_POINTS.toString());
            localDataCenter = properties.getProperty(CassandraConnect.PROPERTIES.LOCAL_DATA_CENTER.toString());
            userName = properties.getProperty(CassandraConnect.PROPERTIES.USER_NAME.toString());
            password = properties.getProperty(CassandraConnect.PROPERTIES.PASSWORD.toString());

            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.PORT.toString())) != null) {
                port = Integer.parseInt(prop);
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.COMPRESSION.toString())) != null) {
                compression = prop.toUpperCase();
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.CORE_CONNECTIONS_PER_HOST.toString())) != null) {
                coreConnectionsPerHost = Integer.parseInt(prop);
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.IDLE_TIMEOUT.toString())) != null ) {
                idleTimeout = Integer.parseInt(prop);
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.MAX_CONNECTIONS_PER_HOST.toString())) != null) {
                maxConnectionsPerHost = Integer.parseInt(prop);
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.MAX_REQUESTS_PER_CONNECTION.toString())) != null) {
                maxRequestsPerConnection = Integer.parseInt(prop);
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.POOL_TIMEOUT.toString())) != null) {
                poolTimeout = Integer.parseInt(prop);
            }
        } catch (Exception e) {
            LOG.warn("Must at least provide one seed for startup ...", e);
        }
    }

    public void configure(CassandraConnect.ConnectionBuilder builder) {
        builder.contactPoints(contactPoints);

        if (port != null) {
            builder.port(port);
        }
        if (userName != null) {
            if(userName.startsWith("enc:")) {
                userName = EncryptionUtils.decrypt(userName.substring(4));
                password = EncryptionUtils.decrypt(password.substring(4));
            }
            builder.userName(userName).password(password);
        }
        if (compression != null) {
            builder.compression(compression);
        }
        if (localDataCenter != null) {
            builder.localDataCenter(localDataCenter);
        }
        if (maxConnectionsPerHost != null) {
            builder.maxConnectionsPerHost(maxConnectionsPerHost);
        }
        if (coreConnectionsPerHost != null) {
            builder.coreConnectionsPerHost(coreConnectionsPerHost);
        }
        if (maxRequestsPerConnection != null) {
            builder.maxSimultaneousRequestsPerConnectionThreshold(maxRequestsPerConnection);
        }
        if (idleTimeout != null) {
            builder.idleTimeoutSeconds(idleTimeout);
        }
        if (poolTimeout != null) {
            builder.poolTimeoutMillis(poolTimeout);
        }
    }
}
