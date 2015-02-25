package net.disbelieve.artemis.cassandra.dao;

import net.disbelieve.artemis.cassandra.CassandraConnect;
import net.disbelieve.artemis.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.artemis.jmx.MXBeansManager;
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
    static Session session;

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
     * @param queue      the blocking queue
     * @param primaryKey the primary key
     * @return the blocking queue containing the mapped models, or, in the case of an error, the Throwable
     */
    public BlockingQueue<Optional> getOne(BlockingQueue<Optional> queue, Object... primaryKey) {
        Statement statement = mapper.getQuery(primaryKey);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.READ));
        ListenableFuture resultFuture = session.executeAsync(statement);
        addCallBack(queue, resultFuture, false);

        return queue;
    }

    /**
     * Gets all records by partition key.
     *
     * @param queue  the blocking queue
     * @param getAll the preparedStatement defining "all" for the model type
     * @return the blocking queue containing the mapped models, or, in the case of an error, the Throwable
     */
    public BlockingQueue<Optional> getAll(BlockingQueue<Optional> queue, BoundStatement getAll) {
        getAll.setConsistencyLevel(getConsistencyLevel(QueryType.READ));
        ResultSetFuture future = session.executeAsync(getAll);
        addCallBack(queue, future, true);

        return queue;
    }

    /**
     * Gets all records by partition key.
     * <p/>
     * Implemented by sub-class. Data in {@code data} which should the be passed to
     * {@link #getAll(java.util.concurrent.BlockingQueue,
     * com.datastax.driver.core.BoundStatement) getAll} for execution
     *
     * @param queue the blocking queue
     * @param data  the object containing information that may be bound to a
     *              {@link com.datastax.driver.core.BoundStatement}
     * @return the blocking queue containing the mapped models, or, in the case of an error, the Throwable
     */
    public abstract BlockingQueue<Optional> getAll(BlockingQueue<Optional> queue, T data);

    /**
     * Inserts/updates (Cassandra makes no distinction) a single record into Cassandra
     *
     * @param queue the blocking queue
     * @param model the model to be written into Cassandra
     *              return the blocking queue passed in by the client used to pass back either the result, or
     *              in the case of an error, the Throwable
     */
    public BlockingQueue<Optional> put(BlockingQueue<Optional> queue, Object model) {
        Statement statement = mapper.saveQuery(model);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));
        ListenableFuture future = session.executeAsync(statement);
        addCallBack(queue, future, true);

        return queue;
    }

    /**
     * Deletes a single record into Cassandra
     *
     * @param queue      the blocking queue
     * @param primaryKey the primary key
     * @return the blocking queue containing the "results" (basically a no-op), or, in the case of an error, the Throwable
     */
    public BlockingQueue<Optional> deleteOne(BlockingQueue<Optional> queue, Object... primaryKey) {
        Statement statement = mapper.deleteQuery(primaryKey);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));
        ListenableFuture future = session.executeAsync(statement);
        addCallBack(queue, future, false);

        return queue;
    }

    /**
     * Deletes all records in a row in Cassandra
     *
     * @param queue     the blocking queue
     * @param deleteAll the preparedStatement defining "all" for the model type
     * @return the blocking queue containing the "results" (basically a no-op), or, in the case of an error, the Throwable
     */
    public BlockingQueue<Optional> deleteAll(BlockingQueue<Optional> queue, BoundStatement deleteAll) {
        deleteAll.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));
        ResultSetFuture future = session.executeAsync(deleteAll);
        addCallBack(queue, future, false);

        return queue;
    }

    /**
     * Executes a {@code BatchStatement}
     *
     * @param queue the blocking queue
     * @param batch the batch containing all the statements to be executed where success is all or none
     * @return the blocking queue containing the "results" (basically a no-op), or, in the case of an error, the Throwable
     */
    public BlockingQueue<Optional> executeBatch(BlockingQueue<Optional> queue, BatchStatement batch) {
        ConsistencyLevel consistencyLevel = getConsistencyLevel(QueryType.WRITE);

        for (Statement statement : batch.getStatements()) {
            statement.setConsistencyLevel(consistencyLevel);
        }
        ResultSetFuture future = session.executeAsync(batch);
        addCallBack(queue, future, true);

        return queue;
    }

    /**
     * Deletes all records by partition key.
     * <p/>
     * Implemented by sub-class. Data in {@code data} should be bound to a
     * {@link com.datastax.driver.core.BoundStatement} which should the be passed to
     * {@link #deleteAll(java.util.concurrent.BlockingQueue, com.datastax.driver.core.BoundStatement) deleteAll} for
     * execution
     *
     * @param queue the blocking queue
     * @param data  the object containing information that may be bound to a
     *              {@link com.datastax.driver.core.BoundStatement}
     * @return the blocking queue passed in by the client used to return the "results" (basically a no-op) or, in the
     * case of an error, the Throwable
     */
    public abstract BlockingQueue<Optional> deleteAll(BlockingQueue<Optional> queue, T data);

    protected void addCallBack(final BlockingQueue queue, ListenableFuture future, final Boolean asListIfOne) {
        Futures.addCallback(future, new FutureCallback() {
            @Override
            public void onSuccess(Object obj) {
                mapResult(queue, obj, asListIfOne);
            }

            @Override
            public void onFailure(Throwable throwable) {
                queue.add(Optional.fromNullable(throwable));
            }
        });
    }

    protected BlockingQueue<Optional> mapResult(BlockingQueue<Optional> queue, Object obj, Boolean asListIfOne) {
        try {
            Optional optional;
            Result results = mapper.map((ResultSet) obj);
            List list = results.all();

            if (!asListIfOne && list.size() < 2) {
                optional = Optional.fromNullable(list.get(0));
            } else {
                optional = Optional.fromNullable(list);
            }
            queue.put(optional);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return queue;
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
