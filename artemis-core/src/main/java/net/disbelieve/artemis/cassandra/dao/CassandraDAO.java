package net.disbelieve.artemis.cassandra.dao;

import net.disbelieve.artemis.cassandra.CassandraConnect;
import net.disbelieve.artemis.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.artemis.jmx.MXBeansManager;
import net.disbelieve.artemis.utils.MappedResult;
import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.BlockingQueue;


/**
 * Created by kmatth002c on 12/10/2014.
 *
 * @param <T> the type parameter
 */
public abstract class CassandraDAO<T> {
    protected MappingManager mappingManager;
    protected Mapper mapper;

    private enum QueryType {
        READ,
        WRITE
    }

    /**
     * The Session.
     */
    protected static Session session;

    /**
     * Instantiates a new Cassandra DAO.
     * <p/>
     * This version is for generic DAOs which could span multiple CFs and need to be added elsewhere.
     * <p/>
     * It is implied that the Mapper needs to be set when used.
     */
    public CassandraDAO() {
        session = CassandraConnect.getSession();
        mappingManager = new MappingManager(session);
    }

    /**
     * Instantiates a new Cassandra DAO.
     * <p/>
     * This version is used for a single CF.
     *
     * @param type the type of the model this DAO will operate on
     */
    public CassandraDAO(Class type) {
        session = CassandraConnect.getSession();
        mappingManager = new MappingManager(session);
        mapper = mappingManager.mapper(type);
    }

    /**
     * Gets a record by the partition and cluster keys
     *
     * @param primaryKey the primary key
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public MappedResult getOne(Object... primaryKey) {
        Statement statement = mapper.getQuery(primaryKey);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.READ));
        ListenableFuture resultFuture = session.executeAsync(statement);
        MappedResult mappedResult = new MappedResult();
        addCallBack(mappedResult, resultFuture, false);

        return mappedResult;
    }

    /**
     * Gets all records by partition key.
     *
     * @param getAll the preparedStatement defining "all" for the model type
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public MappedResult getAll(BoundStatement getAll) {
        getAll.setConsistencyLevel(getConsistencyLevel(QueryType.READ));
        ResultSetFuture future = session.executeAsync(getAll);
        MappedResult mappedResult = new MappedResult();
        addCallBack(mappedResult, future, true);

        return mappedResult;
    }

    /**
     * Gets all records by partition key.
     * <p/>
     * Implemented by sub-class. Data in {@code data} which should the be passed to
     * {@link #getAll(com.datastax.driver.core.BoundStatement) getAll} for execution
     *
     * @param data the object containing information that may be bound to a
     *             {@link com.datastax.driver.core.BoundStatement}
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public abstract MappedResult getAll(T data);

    /**
     * Inserts/updates (Cassandra makes no distinction) a single record into Cassandra
     *
     * @param model the model to be written into Cassandra
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public MappedResult put(Object model) {
        Statement statement = mapper.saveQuery(model);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));
        ListenableFuture future = session.executeAsync(statement);
        MappedResult mappedResult = new MappedResult();
        addCallBack(mappedResult, future, true);

        return mappedResult;
    }

    /**
     * Inserts/updates (Cassandra makes no distinction) a multiple record into Cassandra
     *
     * @param datum the models to be written into Cassandra
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    // This should go somewhere else. It can span multiple column families, and currently each DAO instance is meant
    // to represent one. It's only here because it's repeatable code that might be useful to the end user.
    public MappedResult putAll(Object... datum) {
        BatchStatement batchStatement = new BatchStatement();
        Statement statement;

        for (Object data : datum) {
            mapper = mappingManager.mapper(data.getClass());
            statement = mapper.saveQuery(data);
            batchStatement.add(statement);
        }

        return executeBatch(batchStatement);
    }

    /**
     * Deletes a single record into Cassandra
     *
     * @param primaryKey the primary key
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public MappedResult deleteOne(Object... primaryKey) {
        Statement statement = mapper.deleteQuery(primaryKey);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));
        ListenableFuture future = session.executeAsync(statement);
        MappedResult mappedResult = new MappedResult();
        addCallBack(mappedResult, future, false);

        return mappedResult;
    }

    /**
     * Deletes all records in a row in Cassandra
     *
     * @param deleteAll the preparedStatement defining "all" for the model type
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public MappedResult deleteAll(BoundStatement deleteAll) {
        deleteAll.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));
        ResultSetFuture future = session.executeAsync(deleteAll);
        MappedResult mappedResult = new MappedResult();

        addCallBack(mappedResult, future, false);

        return mappedResult;
    }

    /**
     * Executes a {@code BatchStatement}
     *
     * @param batch the batch containing all the statements to be executed where success is all or none
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    // This should go somewhere else. It can span multiple column families, and currently each DAO instance is meant
    // to represent one. It's only here because it's repeatable code that might be useful to the end user.
    public MappedResult executeBatch(BatchStatement batch) {
        ConsistencyLevel consistencyLevel = getConsistencyLevel(QueryType.WRITE);
        MappedResult mappedResult = new MappedResult();

        for (Statement statement : batch.getStatements()) {
            statement.setConsistencyLevel(consistencyLevel);
        }
        ResultSetFuture future = session.executeAsync(batch);
        addCallBack(mappedResult, future, true);

        return mappedResult;
    }

    /**
     * Deletes all records by partition key.
     * <p/>
     * Implemented by sub-class. Data in {@code data} should be bound to a
     * {@link com.datastax.driver.core.BoundStatement} which should the be passed to
     * {@link #deleteAll(com.datastax.driver.core.BoundStatement) deleteAll} for
     * execution
     *
     * @param data the object containing information that may be bound to a
     *             {@link com.datastax.driver.core.BoundStatement}
     * @return the MappedResult containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    public abstract MappedResult deleteAll(T data);

    protected void addCallBack(final MappedResult mappedResult, ListenableFuture future, final Boolean asListIfOne) {
        Futures.addCallback(future, new FutureCallback() {
            @Override
            public void onSuccess(Object obj) {
                if (obj instanceof ResultSet) {
                    mapResult(mappedResult, obj, asListIfOne);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                mappedResult.setError(throwable);
            }
        });
    }

    protected void mapResult(MappedResult mappedResult, Object obj, Boolean asListIfOne) {
        Result results = mapper.map((ResultSet) obj);
        List list = results.all();

        if (!asListIfOne && list.size() < 2) {
            mappedResult.setResult(list.get(0));
        } else {
            mappedResult.setResult(list);
        }
    }

    protected ConsistencyLevel getConsistencyLevel(QueryType queryType) {
        ConsistencyLevel consistencyLevel = null;

        if (queryType == QueryType.READ) {
            consistencyLevel = ConsistencyLevel.valueOf((String) MXBeansManager.getMXBeanAttribute(
                    CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.READ_CONSISTENCY_LEVEL.toString()));
        } else if (queryType == QueryType.WRITE) {
            consistencyLevel = ConsistencyLevel.valueOf((String) MXBeansManager.getMXBeanAttribute(
                    CassandraMetadataMXBeanImpl.class,
                    CassandraMetadataMXBeanImpl.Attributes.WRITE_CONSISTENCY_LEVEL.toString()));
        }
        return consistencyLevel;
    }
}
