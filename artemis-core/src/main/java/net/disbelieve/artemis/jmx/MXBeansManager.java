package net.disbelieve.artemis.jmx;

import com.datastax.driver.core.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created by kmatth002c on 1/6/2015.
 */
public class MXBeansManager {
    private static MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    private static Logger logger = LoggerFactory.getLogger(MXBeansManager.class);

    public static void registerCassandraMetadata(Cluster metadata) {
        try {
            mbs.registerMBean(new CassandraMetadataMXBeanImpl(metadata), getMXName(CassandraMetadataMXBeanImpl.class));
        } catch (Exception e) {
            logger.error("Could not register MXBean: " + e);
        }
    }

    public static Object getMXBeanAttribute(Class mxBean, String attribute) {
        Object obj = null;
        try {
            obj = mbs.getAttribute(getMXName(mxBean), attribute);
        } catch (Exception e) {
            logger.error("Could not get MXBean attribute: " + e);
        }
        return obj;
    }

    public static void setMXBeanAttribute(Class mxBean, String attributeName, String attributeValue) {
        try {
            mbs.setAttribute(getMXName(mxBean), new Attribute(attributeName, attributeValue));
        } catch (Exception e) {
            logger.error("Could not set MXBean attribute: " + e);
        }
    }

    private static ObjectName getMXName(Class mxBean) {
        ObjectName objectName = null;
        String nameString;
        try {
            nameString = mxBean.getPackage().toString().split(" ")[1];
            nameString += ":name=" + mxBean.getSimpleName();
            objectName = new ObjectName(nameString);
        } catch (Exception e) {
            logger.error("Could not get MXBean name: " + e);
        }
        return objectName;
    }
}
