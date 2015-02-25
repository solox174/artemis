package net.disbelieve.artemis.jmx;

/**
 * Created by kmatth002c on 1/6/2015.
 */
public interface CassandraMetadataMXBean {
    public String getAllHosts();
    public String getKeyspaces();
    public String getColumnFamilies();
    public String getReadConsistencyLevel();
    public String getWriteConsistencyLevel();
    public String getReadConsistencyLevelOverride();
    public String getWriteConsistencyLevelOverride();
    public void setReadConsistencyLevel(String consistencyLevel);
    public void setWriteConsistencyLevel(String consistencyLevel);
    public void setReadConsistencyLevelOverride(String consistencyLevel);
    public void setWriteConsistencyLevelOverride(String consistencyLevel);
}
