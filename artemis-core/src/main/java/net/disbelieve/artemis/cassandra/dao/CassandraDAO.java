package com.comcast.artemis.cassandra.dao;

import com.comcast.artemis.cassandra.CassandraConnect;
import com.comcast.artemis.cassandra.data.Result;
import com.comcast.artemis.exception.ResultAccessException;
import com.comcast.x1.crypt.CryptUtil;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.management.relation.RelationServiceNotRegisteredException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by kmatth002c on 12/10/2014.
 *
 * @param <T> the type parameter
 */
public class CassandraDAO<T> {
    private final static Pattern preparedQueryPattern = Pattern.compile("([<>=]{1,2}|IN).+?(AND|$)");
    protected static final Session session = CassandraConnect.getSession();
    protected MappingManager mappingManager;
    protected Mapper mapper;
    protected String keyspaceName;
    protected String tableName;
    private Map<Integer, PreparedStatement> preparedQueries;
    private List<String> orderedPrimaryKey;
    private List<String> orderedPartitionKey;
    private List<String> orderedClusterKey;
    private CQLUtils cqlUtils;

    /**
     * Instantiates a new Cassandra dAO.
     */
    public CassandraDAO() {
        Type type = this.getClass().getGenericSuperclass();

        mappingManager = new MappingManager(session);

        if (type instanceof ParameterizedType) {
            Type[] types = ((ParameterizedType) type).getActualTypeArguments();
            Class modelClass = (Class) types[0];
            cqlUtils = new CQLUtils(modelClass);
            mapper = mappingManager.mapper(modelClass);
            keyspaceName = cqlUtils.getKeySpace();
            tableName = cqlUtils.getTable();
            orderedPrimaryKey = cqlUtils.getPrimaryKey();
            orderedPartitionKey = cqlUtils.getPartitionKey();
            orderedClusterKey = cqlUtils.getClusterKey();
            preparedQueries = new HashMap<Integer, PreparedStatement>();
        }
    }

    /**
     * Gets a record given it's PRIMARY KEY
     * <p>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     *
     * @param primaryKey the primary key of the record to fetch
     * @return the Result containing the ResultSet mapped to the model type and, in the case of an error,
     * the Throwable
     */
    public Result<T> getOne(Object... primaryKey) {
        Statement statement = mapper.getQuery(primaryKey);
        statement.setConsistencyLevel(cqlUtils.getConsistencyLevel(CQLUtils.QueryType.READ));

        return executeStatement(statement, false, true);
    }

    /**
     * Gets all records in a row given it's  PARTITION KEY.
     * <p>
     * The values provided must correspond to the columns composing the PARTITION
     * KEY (in the order of said partition key).
     * <p>
     *
     * @param partitionKey the partition key of the record to fetch
     * @return the Result containing the ResultSet mapped to the model type and, in the case of an error,
     * the Throwable
     */
    public Result<List<T>> getRow(Object... partitionKey) {
        List<Clause> clauses = new ArrayList<Clause>();
        try {
            for (int i = 0; i < partitionKey.length; i++) {
                clauses.add(QueryBuilder.eq(orderedPartitionKey.get(i), partitionKey[i]));
            }

            return getWhere(clauses);
        } catch (ArrayIndexOutOfBoundsException e) {
            Result result = new Result();
            result.setError(new ResultAccessException(e, "Wrong key count"));

            return result;
        }
    }

    @Deprecated
    public Result<List<T>> getAll(Object... partitionKey) {
        return getRow(partitionKey);
    }

    /**
     * Gets all records in a table
     *
     * @return the Result containing the ResultSet mapped to the model type and, in the case of an error,
     * the Throwable
     */
    public Result<List<T>> getTable() {
        return getTable(null);
    }

    /**
     * Gets all records in a table
     *
     * @param  limit the maximun number of rows to return
     * @return the Result containing the ResultSet mapped to the model type and, in the case of an error,
     * the Throwable
     */
    public Result<List<T>> getTable(Integer limit) {
        Select select = QueryBuilder.select().all().from(keyspaceName, tableName);

        if(limit != null) {
            select.limit(limit);
        }

        return executeStatement(select, true, true);
    }

    /**
     * Get where.
     * <p>
     * Currently only the =,<, and > operands are supported on primary keys (no secondary indexes).
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     *
     * @param whereConditions the conditions of the select
     * @return the result
     */
    public Result<List<T>> getWhere(List<Clause> whereConditions) {
        Select select = QueryBuilder.select().from(keyspaceName, tableName);
        Result<List<T>> result = new Result<List<T>>();

        try {
            Select.Where where = select.where();

            for (Clause condition : whereConditions) {
                where.and(condition);
            }
            BoundStatement boundStatement = bindPreparedStatement(select, whereConditions);
            result = executeStatement(boundStatement, true, true);
        } catch (ResultAccessException e) {
            result.setError(e);
        }
        return result;
    }

    /**
     * Get all records matching matching the conditions determined by which of the primary key fields
     * in the model are set. As such, this function can do getOne, getAll, and getWhere.
     *
     * @param model the model
     * @return the result
     */
    public Result<List<T>> get(T model) {
        Result<List<T>> result = new Result<List<T>>();

        try {
            result = getWhere(cqlUtils.buildClause(orderedPrimaryKey, model));
        } catch (ResultAccessException e) {
            result.setError(e);
        }

        return result;
    }

    /**
     * Inserts/updates a single record into Cassandra
     *
     * @param model the model to be written into Cassandra
     * <p>
     * fields annotated with @Secure will be encrypted prior to write
     * <p>
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result putOne(Object model) {
        CryptUtil.encrypt(model);
        Statement statement = mapper.saveQuery(model);
        statement.setConsistencyLevel(cqlUtils.getConsistencyLevel(CQLUtils.QueryType.WRITE));

        return executeStatement(statement, false, false);
    }

    /**
     * Inserts/updates (Cassandra makes no distinction) a multiple models into Cassandra
     *
     * @param datum the models to be written into Cassandra - NO ENCRYPTION SUPPORT
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     * @deprecated
     */
    // This should go somewhere else. It can span multiple column families, and currently each DAO instance is meant
    // to represent one. It's only here because it's repeatable code that might be useful to the end user.
    @Deprecated
    public Result putAll(Object... datum) {
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
     * Deletes a single record given it's PRIMARY KEY
     * <p>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     *
     * @param primaryKey the primary key for the record to be deleted
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result<T> deleteOne(Object... primaryKey) {
        Statement statement = mapper.deleteQuery(primaryKey);
        statement.setConsistencyLevel(cqlUtils.getConsistencyLevel(CQLUtils.QueryType.WRITE));

        return executeStatement(statement, false, false);
    }

    /**
     * Deletes all records in a row given it's PARTITION KEY
     * <p>
     * The values provided must correspond to the columns composing the PARTITION
     * KEY (in the order of said primary key).
     * <p>
     *
     * @param partitionKey the partitionKey of the row to delete
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result<List<T>> deleteRow(String... partitionKey) {
        List<Clause> clauses = new ArrayList<Clause>();

        try {
            for (int i = 0; i < partitionKey.length; i++) {
                clauses.add(QueryBuilder.eq(orderedPartitionKey.get(i), partitionKey[i]));
            }

            return deleteWhere(clauses);
    } catch (ArrayIndexOutOfBoundsException e) {
        Result result = new Result();
        result.setError(new ResultAccessException(e, "Wrong key count"));

        return result;
    }
    }

    @Deprecated
    public Result<List<T>> deleteAll(String... partitionKey) {
        return deleteRow(partitionKey);
    }

    /**
     * Delete where.
     * <p>
     * Currently only the =,<, and > operands are supported on primary keys (no secondary indexes).
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     *
     * @param whereConditions the conditions of the delete.
     * @return the result
     */
    public Result<List<T>> deleteWhere(List<Clause> whereConditions) {
        Delete delete = QueryBuilder.delete().from(keyspaceName, tableName);
        Result<List<T>> result = new Result<List<T>>();

        try {
            Delete.Where where = delete.where();

            for (Clause condition : whereConditions) {
                where.and(condition);
            }
            BoundStatement boundStatement = bindPreparedStatement(delete, whereConditions);
            result = executeStatement(boundStatement, false, false);
        } catch (ResultAccessException e) {
            result.setError(e);
        }
        return result;
    }

    /**
     * Delete all records matching matching the conditions determined by which of the primary key fields
     * in the model are set. As such, this function can do getOne, getAll, and getWhere.
     *
     * @param model the model
     * @return the result
     */
    public Result<List<T>> delete(T model) {
        Result<List<T>> result = new Result<List<T>>();

        try {
            result = deleteWhere(cqlUtils.buildClause(orderedPrimaryKey, model));
        } catch (ResultAccessException e) {
            result.setError(e);
        }

        return result;
    }

    /**
     * Gets count of rows matching whereClause.
     * Warning: this method can be very expensive and may time-out.
     *
     * @param whereConditions the where clause
     * @param limit           maximum number of rows to count
     * @return the count
     */
    public Result<Long> getCount(List<Clause> whereConditions, Integer limit) {
        Select select = QueryBuilder.select().countAll().from(keyspaceName, tableName).limit(limit);
        final Result result = new Result();
        ResultSetFuture future;

        try {
            Select.Where where = select.where();

            for(Clause clause : whereConditions) {
                where.and(clause);
            }
            future = session.executeAsync(bindPreparedStatement(select, whereConditions));

            Futures.addCallback(future, new FutureCallback() {
                public void onSuccess(Object obj) {
                    result.setUnmappedResultSet((ResultSet) obj);
                    result.setMappedResult(((ResultSet) obj).one().getLong("count"));
                }

                public void onFailure(Throwable throwable) {
                    result.setError(throwable);
                }
            });
        } catch (ResultAccessException e) {
            result.setError(e);
        }

        return result;
    }

    /**
     * Executes a {@code BatchStatement}
     *
     * @param batch the batch containing all the statements to be executed where success is all or none
     * @return the Result containing the ResultSet mapped to the model type, or, in the case of an error,
     * the Throwable
     */
    // This should go somewhere else. It can span multiple column families, and currently each DAO instance is meant
    // to represent one. It's only here because it's repeatable code that might be useful to the end user.
    public Result executeBatch(BatchStatement batch) {
        ConsistencyLevel consistencyLevel = CQLUtils.getConsistencyLevel(CQLUtils.QueryType.WRITE);

        for (Statement statement : batch.getStatements()) {
            statement.setConsistencyLevel(consistencyLevel);
        }
        return executeStatement(batch, false, false);
    }

    /**
     * Execute statement.
     *
     * @param statement   the statement
     * @param asListIfOne flag to return as a list even if there is only one record
     * @param mapResult   flag to turn "ORM" mapping on or off
     * @return the result
     */
    private Result executeStatement(Statement statement, boolean asListIfOne, boolean mapResult) {
        Result result = new Result();
        ResultSetFuture future = session.executeAsync(statement);

        addCallBack(result, future, asListIfOne, mapResult);

        return result;
    }

    private void addCallBack(final Result result, ListenableFuture future, final Boolean asListIfOne, final boolean mapResult) {
        Futures.addCallback(future, new FutureCallback() {
            public void onSuccess(Object obj) {
                if (obj instanceof ResultSet) {
                    result.setUnmappedResultSet((ResultSet) obj);

                    if (mapResult) {
                        mapResult(result, obj, asListIfOne);
                    } else {
                        result.setMappedResult(null);
                    }
                }
            }

            public void onFailure(Throwable throwable) {
                result.setError(throwable);
            }
        });
    }

    private void mapResult(Result result, Object obj, Boolean asListIfOne) {
        com.datastax.driver.mapping.Result results = mapper.map((ResultSet) obj);
        List list = results.all();

        for(int i = 0; i < list.size(); i++) {
            CryptUtil.decrypt(list.get(i));
        }

        if (!asListIfOne && list.size() == 1) {
            result.setMappedResult(list.get(0));
        } else if (!list.isEmpty()) {
            result.setMappedResult(list);
        } else {
            result.setMappedResult(null);
        }
    }

    private BoundStatement bindPreparedStatement(BuiltStatement statement, List<Clause> whereConditions) throws ResultAccessException {
        if (whereConditions != null && !whereConditions.isEmpty()) {
            BoundStatement boundPreparedStatement;
            Matcher preparedQueryMatcher = preparedQueryPattern.matcher(statement.getQueryString());
            String preparedQueryString = preparedQueryMatcher.replaceAll("$1 \\? $2");
            int hash = preparedQueryString.hashCode();
            PreparedStatement preparedQuery;
            List values = new ArrayList();

            synchronized (preparedQueries) {
                if ((preparedQuery = preparedQueries.get(hash)) == null) {
                    preparedQuery = session.prepare(preparedQueryString);
                    preparedQueries.put(hash, preparedQuery);
                }
            }
            boundPreparedStatement = new BoundStatement(preparedQuery);

            for(Clause clause: whereConditions) {
                values.add(ClauseExtractor.getValues(clause));
            }
            boundPreparedStatement.bind(values.toArray());

            return boundPreparedStatement;
        } else {
            throw new ResultAccessException("Not a valid conditional query");
        }
    }
}
