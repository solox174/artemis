package net.disbelieve.artemis.jersey;

import net.disbelieve.artemis.cassandra.CassandraConnect;
import net.disbelieve.artemis.jersey.filter.CassandraConsistencyLevelFilter;
import net.disbelieve.artemis.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.artemis.jmx.MXBeansManager;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kmatth002c on 12/10/2014.
 */

public
@ApplicationPath("/*")
class ArtemisApplication extends ResourceConfig {
    private static Logger logger = LoggerFactory.getLogger(ArtemisApplication.class);
    private CassandraConnect cassandra;
    private Properties properties;
    private String contactPoints;
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
        } catch (IOException e) {
            logger.info("Could not load cassandra.properties", e);
        }
        try {
            //File jarLoc = new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
            String overridePropertyLoc = System.getProperties().getProperty("config");
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
            contactPoints = properties.getProperty(CassandraConnect.PROPERTIES.CONTACT_POINTS.toString());
            compression = properties.getProperty(CassandraConnect.PROPERTIES.COMPRESSION.toString()).toUpperCase();
            localDataCenter = properties.getProperty(CassandraConnect.PROPERTIES.LOCAL_DATA_CENTER.toString());
            writeConsistency = properties.getProperty(CassandraConnect.PROPERTIES.WRITE_CONSISTENCY_LEVEL.toString()).toUpperCase();
            readConsistency = properties.getProperty(CassandraConnect.PROPERTIES.READ_CONSISTENCY_LEVEL.toString()).toUpperCase();
            userName = properties.getProperty(CassandraConnect.PROPERTIES.USER_NAME.toString());
            password = properties.getProperty(CassandraConnect.PROPERTIES.PASSWORD.toString());
        } catch (Exception e) {
            logger.warn("Could not load all the required cassandra.properties", e);
        }
    }

    public ArtemisApplication() {
        //property(EntityFilteringFeature.ENTITY_FILTERING_SCOPE,
        //        new Annotation[] {SecurityAnnotations.rolesAllowed("user", "admin")});
        //register(SecurityEntityFilteringFeature.class);

        register(JacksonFeature.class);
        register(CassandraConsistencyLevelFilter.class);
        //register(CrossOriginFilter.class);
        /** place in implementing service to enable OAuth security for your application*/
        //register(SecurityContextFilter.class);

        packages("net.disbelieve.artemis.jersey");

        cassandra = new CassandraConnect.ConnectionBuilder()
                .contactPoints(contactPoints)
                .localDataCenter(localDataCenter)
                .userName(userName)
                .password(password)
                .compression(compression).build();
        cassandra.connect();

        MXBeansManager.registerCassandraMetadata(cassandra.getSession().getCluster());
        MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL.toString(),
                writeConsistency);
        MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL.toString(),
                readConsistency);
    }
}
