package net.disbelieve.artemis.cassandra.data;

/**
 * Created by kmatth002c on 2/12/2015.
 */
public abstract class CassandraData {
    abstract public long getUpdated();

    abstract public void setUpdated(long updated);

    abstract public long getTtl();

    abstract public void setTtl(long ttl);
}
