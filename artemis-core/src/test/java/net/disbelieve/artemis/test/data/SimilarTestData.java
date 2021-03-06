package com.comcast.artemis.test.data;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by kmatth207 on 5/22/2015.
 */
@Table(keyspace = "artemisKeySpace", name = "testColumnFamily", caseSensitiveKeyspace = true, caseSensitiveTable = true)
public class SimilarTestData {
    @PartitionKey
    private String partitionKey;
    @ClusteringColumn
    private String clusterKey;
    private String similarData;

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getClusterKey() {
        return clusterKey;
    }

    public void setClusterKey(String clusterKey) {
        this.clusterKey = clusterKey;
    }

    public String getSimilarData() {
        return similarData;
    }

    public void setSimilarData(String similarData) {
        this.similarData = similarData;
    }
}
