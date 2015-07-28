package com.comcast.artemis.jersey;

import com.comcast.artemis.cassandra.CassandraConnect;
import com.comcast.artemis.cassandra.dao.CassandraDAO;
import com.comcast.artemis.cassandra.dao.Repository;
import com.comcast.artemis.exception.ConnectionException;
import com.comcast.artemis.jersey.filter.CassandraConsistencyLevelFilter;
import com.comcast.artemis.jmx.CassandraMetadataMXBeanImpl;
import com.comcast.artemis.jmx.MXBeansManager;
import com.comcast.artemis.utils.ClassUtils;
import com.comcast.artemis.utils.CassandraConnectUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by kmatth002c on 12/10/2014.
 */

public class ArtemisApplication extends ResourceConfig {
    private static final Logger LOG = LoggerFactory.getLogger(ArtemisApplication.class);
    private static final HashMap daos = new HashMap<Class,CassandraDAO>();
    private CassandraConnect cassandra;
    protected Properties properties;

    public ArtemisApplication() {
        CassandraConnect.ConnectionBuilder builder;
        CassandraConnectUtils connectUtils;
        Properties propertiesDefault = new Properties();
        InputStream propertyStream = null;
        String writeConsistency, readConsistency;

        try {
            propertyStream = getClass().getClassLoader().getResourceAsStream("cassandra.properties");
            propertiesDefault.load(propertyStream);
        } catch (IOException ioe) {
            LOG.info("Could not load cassandra.properties", ioe);
        } catch (Exception e) {
            LOG.info("Could not load cassandra.properties", e);
        } finally {
            if(propertyStream != null) {
                try {
                    propertyStream.close();
                } catch (IOException e) {
                    LOG.info("Could not close property stream", e);
                }
            }
        }
        try {
            String overridePropertyLoc = System.getProperties().getProperty("cql3Config");
            properties = new Properties(propertiesDefault);

            if (overridePropertyLoc != null) {
                FileInputStream overridePropertyStream = new FileInputStream(overridePropertyLoc);
                properties.load(overridePropertyStream);
            }
        } catch (FileNotFoundException e) {
            LOG.info("Could not find cassandra.properties override", e);
        } catch (IOException e) {
            LOG.info("Could not load cassandra.properties override", e);
        }
        builder = new CassandraConnect.ConnectionBuilder();
        connectUtils = new CassandraConnectUtils(properties);
        connectUtils.configure(builder);
        cassandra = builder.build();

        try {
            cassandra.connect();
            List<Class> classes = ClassUtils.getAnnotatedWith("com.comcast", Repository.class);

            for(Class clazz : classes) {
                try {
                    daos.put(clazz, clazz.newInstance());
                } catch (InstantiationException e) {
                    LOG.error("Could not create a DAO", e);
                } catch (IllegalAccessException e) {
                    LOG.error("Could not create a DAO", e);
                }
            }
        } catch (ConnectionException e) {
            LOG.error("Could not connect to Cassandra", e);
        }
        readConsistency = properties.getProperty(CassandraConnect.PROPERTIES.READ_CONSISTENCY_LEVEL.toString());
        writeConsistency = properties.getProperty(CassandraConnect.PROPERTIES.WRITE_CONSISTENCY_LEVEL.toString());

        if(writeConsistency != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL.toString(),
                    writeConsistency.toUpperCase());
        }
        if(readConsistency != null) {
            MXBeansManager.setMXBeanAttribute(CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL.toString(),
                    readConsistency.toUpperCase());
        }
        register(JacksonFeature.class);
        register(CassandraConsistencyLevelFilter.class);
        packages("com.comcast.artemis.jersey");
    }

    public static CassandraDAO getDAO(Class daoType) {
        return (CassandraDAO) daos.get(daoType);
    }
}
