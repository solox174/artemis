package com.comcast.artemis.test.data;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by kmatth207 on 5/22/2015.
 */
@Table(keyspace = "artemisKeySpace", name = "testLongCompoundColumnFamily", caseSensitiveKeyspace = true, caseSensitiveTable = true)
public class TestLongCompoundKey {
    @PartitionKey(0)
    private Long partitionKey1;
    @PartitionKey(1)
    private Long partitionKey2;
    @ClusteringColumn(0)
    private Long clusterKey1;
    @ClusteringColumn(1)
    private Long clusterKey2;
    private Long data;

    public Long getPartitionKey1() {
        return partitionKey1;
    }

    public void setPartitionKey1(Long partitionKey1) {
        this.partitionKey1 = partitionKey1;
    }

    public Long getPartitionKey2() {
        return partitionKey2;
    }

    public void setPartitionKey2(Long partitionKey2) {
        this.partitionKey2 = partitionKey2;
    }

    public Long getClusterKey1() {
        return clusterKey1;
    }

    public void setClusterKey1(Long clusterKey1) {
        this.clusterKey1 = clusterKey1;
    }

    public Long getClusterKey2() {
        return clusterKey2;
    }

    public void setClusterKey2(Long clusterKey2) {
        this.clusterKey2 = clusterKey2;
    }

    public Long getData() {
        return data;
    }

    public void setData(Long data) {
        this.data = data;
    }
}
