package net.disbelieve.wicca.jmx;

/**
 * Created by kmatth002c on 1/6/2015.
 */
public interface CassandraMetadataMXBean {
    public String getAllHosts();
    public String getKeyspaces();
    public String getColumnFamilies();
    public String getReadConsistencyLevel();
    public String getWriteConsistencyLevel();
    public void setReadConsistencyLevel(String consistencyLevel);
    public void setWriteConsistencyLevel(String consistencyLevel);
}
