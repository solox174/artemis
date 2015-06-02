package net.disbelieve.artemis.jersey;

import net.disbelieve.artemis.cassandra.CassandraConnect;
import net.disbelieve.artemis.jersey.filter.CassandraConsistencyLevelFilter;
import net.disbelieve.artemis.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.artemis.jmx.MXBeansManager;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kmatth002c on 12/10/2014.
 */

public
class ArtemisApplication extends ResourceConfig {
    private static Logger logger = LoggerFactory.getLogger(ArtemisApplication.class);
    private CassandraConnect cassandra;
    protected Properties properties;
    private String contactPoints;
    private Integer port;
    private String compression;
    private String localDataCenter;
    private String writeConsistency;
    private String readConsistency;
    private String userName;
    private String password;

    {
        Properties propertiesDefault = new Properties();
        try {
            InputStream propertyStream = getClass().getClassLoader().getResourceAsStream("cassandra.properties");
            propertiesDefault.load(propertyStream);
        } catch (IOException ioe) {
            logger.info("Could not load cassandra.properties", ioe);
        } catch (Exception e) {
            logger.info("Could not load cassandra.properties", e);
        }

        try {
            String overridePropertyLoc = System.getProperties().getProperty("cql3Config");
            properties = new Properties(propertiesDefault);

            if (overridePropertyLoc != null) {
                FileInputStream overridePropertyStream = new FileInputStream(overridePropertyLoc);
                properties.load(overridePropertyStream);
            }
        } catch (FileNotFoundException e) {
            logger.info("Could not find cassandra-override.properties", e);
        } catch (IOException e) {
            logger.info("Could not load cassandra-override.properties", e);
        }
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
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.COMPRESSION.toString())) != null) {
                compression = prop.toUpperCase();
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.WRITE_CONSISTENCY_LEVEL.toString())) != null) {
                writeConsistency = prop.toUpperCase();
            }
            if((prop = properties.getProperty(CassandraConnect.PROPERTIES.READ_CONSISTENCY_LEVEL.toString())) != null) {
                readConsistency = prop.toUpperCase();
            }
        } catch (Exception e) {
            logger.warn("Must at least provide one seed for startup ...", e);
        }
    }

    public ArtemisApplication() {
        register(JacksonFeature.class);
        register(CassandraConsistencyLevelFilter.class);
        packages("net.disbelieve.artemis.jersey");

        CassandraConnect.ConnectionBuilder builder = new CassandraConnect.ConnectionBuilder().contactPoints(contactPoints);
        if (port != null) {
            builder.port(port);
        }
        if (userName != null) {
            builder.userName(userName)
                    .password(password);
        }
        if (compression != null) {
            builder.compression(compression);
        }
        if (localDataCenter != null) {
            builder.localDataCenter(localDataCenter);
        }
        cassandra = builder.build();
        cassandra.connect();

        MXBeansManager.registerCassandraMetadata(cassandra.getSession().getCluster());
        if(writeConsistency != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL.toString(),
                    writeConsistency);
        }
        if(readConsistency != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL.toString(),
                    readConsistency);
        }
    }
}
