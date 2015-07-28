package com.comcast.artemis.test;

import com.comcast.artemis.cassandra.data.ModelUtils;
import com.comcast.artemis.test.data.SimilarTestData;
import com.comcast.artemis.test.data.TestStringSimpleKey;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CassandraModelTest {
    private static TestStringSimpleKey rowOne;

    @BeforeClass
    public static void setupModel() throws Exception {
        rowOne = new TestStringSimpleKey();
        rowOne.setPartitionKey("pkey1");
        rowOne.setClusterKey("ckey1");
        rowOne.setData("data1");
    }

    @Test
    public void _0_put() throws Exception {
        ModelUtils modelUtils = new ModelUtils();
        SimilarTestData similarTestData = (SimilarTestData) modelUtils.transcribe(rowOne, SimilarTestData.class);

        assertEquals(rowOne.getPartitionKey(), similarTestData.getPartitionKey());
        assertEquals(rowOne.getClusterKey(), similarTestData.getClusterKey());
        assertNotEquals(rowOne.getData(), similarTestData.getSimilarData());
    }
}
