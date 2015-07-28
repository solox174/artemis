package com.comcast.artemis.test;

import com.comcast.artemis.cassandra.CassandraConnect;
import com.comcast.artemis.cassandra.data.Result;
import com.comcast.artemis.jersey.ArtemisApplication;
import com.comcast.artemis.test.dao.TestLongCompoundDAO;
import com.comcast.artemis.test.dao.TestStringSimpleDAO;
import com.comcast.artemis.test.data.TestLongCompoundKey;
import com.comcast.artemis.test.data.TestStringSimpleKey;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CassandraDAOTest {
    private static TestStringSimpleDAO testStringSimpleDAO;
    private static TestLongCompoundDAO testLongCompoundDAO;
    private static ArtemisApplication artemisApplication;
    private static TestStringSimpleKey tsskOne;
    private static TestStringSimpleKey tsskTwo;
    private static TestLongCompoundKey tlckOne;

    @BeforeClass
    public static void setupDataBase() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(30000L);
        artemisApplication = new ArtemisApplication();
        CassandraConnect.getSession().execute("create keyspace \"artemisKeySpace\" with REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

        Create createStringSimpleTable = SchemaBuilder.createTable("\"artemisKeySpace\"", "\"testStringSimpleColumnFamily\"");
        createStringSimpleTable.addPartitionKey("partitionKey", DataType.ascii());
        createStringSimpleTable.addClusteringColumn("clusterKey", DataType.ascii());
        createStringSimpleTable.addColumn("data", DataType.ascii());
        CassandraConnect.getSession().execute(createStringSimpleTable);

        Create createLongCompoundTable = SchemaBuilder.createTable("\"artemisKeySpace\"", "\"testLongCompoundColumnFamily\"");
        createLongCompoundTable.addPartitionKey("partitionKey1", DataType.bigint());
        createLongCompoundTable.addPartitionKey("partitionKey2", DataType.bigint());
        createLongCompoundTable.addClusteringColumn("clusterKey1", DataType.bigint());
        createLongCompoundTable.addClusteringColumn("clusterKey2", DataType.bigint());
        createLongCompoundTable.addColumn("data", DataType.bigint());
        CassandraConnect.getSession().execute(createLongCompoundTable);

        testStringSimpleDAO = (TestStringSimpleDAO) ArtemisApplication.getDAO(TestStringSimpleDAO.class);
        testLongCompoundDAO = (TestLongCompoundDAO) ArtemisApplication.getDAO(TestLongCompoundDAO.class);

        tsskOne = new TestStringSimpleKey();
        tsskOne.setPartitionKey("p key1");
        tsskOne.setClusterKey("ckey1");
        tsskOne.setData("data1");

        tsskTwo = new TestStringSimpleKey();
        tsskTwo.setPartitionKey("p key1");
        tsskTwo.setClusterKey("ckey2");
        tsskTwo.setData("data1");

        tlckOne = new TestLongCompoundKey();
        tlckOne.setPartitionKey1(1l);
        tlckOne.setPartitionKey2(0l);
        tlckOne.setClusterKey1(8l);
        tlckOne.setClusterKey2(9l);
        tlckOne.setData(5l);
    }

    @Test
    public void _00_putOneString() throws Exception {
        Result result = testStringSimpleDAO.putOne(tsskOne);
        ResultSet resultSet = result.getUnmappedResult();

        assertEquals(true, resultSet.wasApplied());
    }
    @Test
    public void _01_getOne() throws Exception {
        Result<TestStringSimpleKey> result = testStringSimpleDAO.getOne(tsskOne.getPartitionKey(), tsskOne.getClusterKey());
        TestStringSimpleKey testStringSimpleKey = result.getMappedResult();

        assertEquals(tsskOne.getPartitionKey(), testStringSimpleKey.getPartitionKey());
    }

    @Test
    public void _02_deleteOne() throws Exception {
        Result<TestStringSimpleKey> result = testStringSimpleDAO.deleteOne(tsskOne.getPartitionKey(), tsskOne.getClusterKey());
        ResultSet resultSet = result.getUnmappedResult();
        result = testStringSimpleDAO.getOne(tsskOne.getPartitionKey(), tsskOne.getClusterKey());
        TestStringSimpleKey testStringSimpleKey = result.getMappedResult();

        assertEquals(null, testStringSimpleKey);
    }

    @Test
    public void _03_putAll() throws Exception {
        Result<TestStringSimpleKey> result = testStringSimpleDAO.putAll(tsskOne, tsskTwo);
        result.getUnmappedResult();
        result = testStringSimpleDAO.getOne(tsskTwo.getPartitionKey(), tsskTwo.getClusterKey());
        TestStringSimpleKey testStringSimpleKey = result.getMappedResult();

        assertEquals("ckey2", testStringSimpleKey.getClusterKey());
    }

    @Test
    public void _04_getCount() throws Exception {
        Clause clause = QueryBuilder.eq("partitionKey", "p key1");
        List clauses = new ArrayList<Clause>();
        clauses.add(clause);
        Result<Long> result = testStringSimpleDAO.getCount(clauses, 5);
        Long count = result.getMappedResult();

        assertEquals(2, count.intValue());
    }

    @Test
    public void _05_delete() throws Exception {
        Result result = testStringSimpleDAO.delete(tsskOne);
        Clause clause = QueryBuilder.eq("partitionKey", "p key1");
        List clauses = new ArrayList<Clause>();

        clauses.add(clause);
        result.getMappedResult();
        result = testStringSimpleDAO.getCount(clauses, 5);

        assertEquals(1l, result.getMappedResult());
    }

    @Test
    public void _06_getAll() throws Exception {
        Result<List<TestStringSimpleKey>> result = testStringSimpleDAO.getAll(tsskOne.getPartitionKey());
        List<TestStringSimpleKey> testDatum = result.getMappedResult();

        assertEquals(1, testDatum.size());
    }

    @Test
    public void _07_getWhereIn() throws Exception {
        List<Clause> whereConditions = new ArrayList<>();
        whereConditions.add(QueryBuilder.in("partitionKey", "p key1"));
        Result<List<TestStringSimpleKey>> result = testStringSimpleDAO.getWhere(whereConditions);
        List<TestStringSimpleKey> testDatum = result.getMappedResult();

        assertEquals(1, testDatum.size());
    }
    @Test
    public void _07_putOneLone() throws Exception {
        Result result = testLongCompoundDAO.putOne(tlckOne);
        ResultSet resultSet = result.getUnmappedResult();

        assertEquals(true, resultSet.wasApplied());
    }
    @Test
    public void _08_getWhereGTE() throws Exception {
        List<Clause> whereConditions = new ArrayList<>();
        whereConditions.add(QueryBuilder.eq("partitionKey1", 1l));
        whereConditions.add(QueryBuilder.eq("partitionKey2", 0l));
        whereConditions.add(QueryBuilder.gte("clusterKey1", 4l));
        Result<List<TestLongCompoundKey>> result = testLongCompoundDAO.getWhere(whereConditions);
        List<TestLongCompoundKey> testDatum = result.getMappedResult();

        assertEquals(1, testDatum.size());
    }

    @Test
    public void _09_get() throws Exception {
        Result<List<TestStringSimpleKey>> result = testStringSimpleDAO.get(tsskTwo);
        List<TestStringSimpleKey> testDatum = result.getMappedResult();

        assertEquals(1, testDatum.size());
        assertEquals("data1", testDatum.get(0).getData());
    }

    @Test
    public void _10_deleteAll() throws Exception {
        Result<List<TestStringSimpleKey>> result = testStringSimpleDAO.deleteAll(tsskOne.getPartitionKey());
        result.getUnmappedResult();
        result = testStringSimpleDAO.getAll(tsskOne.getPartitionKey());
        List<TestStringSimpleKey> testDatum = result.getMappedResult();

        assertEquals(null, testDatum);
    }
}
