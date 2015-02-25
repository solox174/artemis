package net.disbelieve.artemis.jmx;

import com.datastax.driver.core.*;

/**
 * Created by kmatth002c on 1/5/2015.
 */
public class CassandraMetadataMXBeanImpl implements CassandraMetadataMXBean {
    private Cluster cluster;
    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;
    private ConsistencyLevel readConsistencyLevelOverride;
    private ConsistencyLevel writeConsistencyLevelOverride;
    public static enum Attributes {
        READ_CONSISTENCY_LEVEL("ReadConsistencyLevel"),
        WRITE_CONSISTENCY_LEVEL("WriteConsistencyLevel"),
        READ_CONSISTENCY_LEVEL_OVERRIDE("ReadConsistencyLevelOverride"),
        WRITE_CONSISTENCY_LEVEL_OVERRIDE("WriteConsistencyLevelOverride");
        private final String name;

        private Attributes(String s) {
            name = s;
        }

        public String toString(){
            return name;
        }

    }

    public CassandraMetadataMXBeanImpl() {}

    public CassandraMetadataMXBeanImpl(Cluster cluster) {
        this.cluster = cluster;
        this.readConsistencyLevel = cluster.getConfiguration().getQueryOptions().getConsistencyLevel();
        this.writeConsistencyLevel = cluster.getConfiguration().getQueryOptions().getConsistencyLevel();
    }
    public String getAllHosts() {
        StringBuffer sb = new StringBuffer();

        for(Host host : cluster.getMetadata().getAllHosts()) {
           sb.append(host.toString());
        }
        return sb.toString();
    }

    public String getKeyspaces() {
        StringBuffer sb = new StringBuffer();

        for(KeyspaceMetadata km : cluster.getMetadata().getKeyspaces()) {
            sb.append(km.exportAsString());
        }
        return sb.toString();
    }

    public String getColumnFamilies() {
        StringBuffer sb = new StringBuffer();

        for(KeyspaceMetadata km : cluster.getMetadata().getKeyspaces()) {
            for(TableMetadata tbl : km.getTables()) {
                sb.append(tbl.exportAsString());
            }
        }
        return sb.toString();

    }
    public String getReadConsistencyLevel() {
        return readConsistencyLevelOverride == null ? readConsistencyLevel.toString() :
                readConsistencyLevelOverride.toString();
    }

    public void setReadConsistencyLevel(String consistencyLevel) {
        readConsistencyLevel = ConsistencyLevel.valueOf(ConsistencyLevel.class, consistencyLevel);
    }

    public String getWriteConsistencyLevel() {
        return writeConsistencyLevelOverride == null ? writeConsistencyLevel.toString() :
                writeConsistencyLevelOverride.toString();
    }

    public String getReadConsistencyLevelOverride() {
        return readConsistencyLevelOverride.toString();
    }

    public String getWriteConsistencyLevelOverride() {
        return writeConsistencyLevelOverride.toString();
    }

    public void setWriteConsistencyLevel(String consistencyLevel) {
        writeConsistencyLevel = ConsistencyLevel.valueOf(ConsistencyLevel.class, consistencyLevel);
    }

    public void setReadConsistencyLevelOverride(String consistencyLevel) {
        readConsistencyLevelOverride = ConsistencyLevel.valueOf(ConsistencyLevel.class, consistencyLevel);
    }

    public void setWriteConsistencyLevelOverride(String consistencyLevel) {
        writeConsistencyLevelOverride = ConsistencyLevel.valueOf(ConsistencyLevel.class, consistencyLevel);
    }
}
