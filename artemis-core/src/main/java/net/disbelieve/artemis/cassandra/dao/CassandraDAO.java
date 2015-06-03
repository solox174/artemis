package net.disbelieve.artemis.cassandra.dao;

import net.disbelieve.artemis.cassandra.CassandraConnect;
import net.disbelieve.artemis.cassandra.data.Result;
import net.disbelieve.artemis.exception.ResultAccessException;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by kmatth002c on 12/10/2014.
 *
 * @param <T> the type parameter
 */
public class CassandraDAO<T> {
    protected MappingManager mappingManager;
    protected Mapper mapper;
    protected String keyspaceName;
    protected String tableName;
    protected static Session session;
    private Map<Integer, PreparedStatement> preparedQueries;
    private List<String> orderedPrimaryKey;
    private List<String> orderedPartitionKey;
    private List<String> orderedClusterKey;
    private CQLUtils cqlUtils;

    public CassandraDAO() {
        Type type = this.getClass().getGenericSuperclass();

        session = CassandraConnect.getSession();
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
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
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
     * <p/>
     * The values provided must correspond to the columns composing the PARTITION
     * KEY (in the order of said partition key).
     * <p/>
     *
     * @param partitionKey the partition key of the record to fetch
     * @return the Result containing the ResultSet mapped to the model type and, in the case of an error,
     * the Throwable
     */
    public Result<List<T>> getAll(String... partitionKey) {
        Select select = QueryBuilder.select().from(keyspaceName + "." + tableName);
        List<Clause> clauses = new ArrayList<Clause>();
        Result<List<T>> result = null;

        if(partitionKey.length == orderedPartitionKey.size()) {
            for(int i = 0; i < partitionKey.length; i++) {
                clauses.add(QueryBuilder.eq(orderedPartitionKey.get(i), partitionKey[i]));
            }
        }
        try {
            BoundStatement boundStatement = bindPreparedStatement(select, clauses);
            result = executeStatement(boundStatement, true, true);
        } catch (ResultAccessException e) {
            result.setError(e);
        }
        return result;
    }

    /**
     * Get where.
     * <p/>
     * Currently only the =,<, and > operands are supported on primary keys (no secondary indexes).
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
     * @param whereConditions the conditions of the select
     * @return the result
     */
    public Result<List<T>> getWhere(List<Clause> whereConditions) {
        Select select = QueryBuilder.select().from(keyspaceName + "." + tableName);
        Result<List<T>> result = null;

        try {
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
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result putOne(Object model) {
        Statement statement = mapper.saveQuery(model);
        statement.setConsistencyLevel(cqlUtils.getConsistencyLevel(CQLUtils.QueryType.WRITE));

        return executeStatement(statement);
    }

    /**
     * Inserts/updates (Cassandra makes no distinction) a multiple models into Cassandra
     *
     * @param datum the models to be written into Cassandra
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    // This should go somewhere else. It can span multiple column families, and currently each DAO instance is meant
    // to represent one. It's only here because it's repeatable code that might be useful to the end user.
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
     * <p/>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
     *
     * @param primaryKey the primary key for the record to be deleted
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result<T> deleteOne(Object... primaryKey) {
        Statement statement = mapper.deleteQuery(primaryKey);
        statement.setConsistencyLevel(cqlUtils.getConsistencyLevel(CQLUtils.QueryType.WRITE));

        return executeStatement(statement);
    }

    /**
     * Deletes all records in a row given it's PARTITION KEY
     * <p/>
     * The values provided must correspond to the columns composing the PARTITION
     * KEY (in the order of said primary key).
     * <p/>
     *
     * @param partitionKey the partitionKey of the row to delete
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result<List<T>> deleteAll(String... partitionKey) {
        Delete delete = QueryBuilder.delete().from(keyspaceName + "." + tableName);
        List<Clause> clauses = new ArrayList<Clause>();
        Result<List<T>> result = null;

        if(partitionKey.length == orderedPartitionKey.size()) {
            for(int i = 0; i < partitionKey.length; i++) {
                clauses.add(QueryBuilder.eq(orderedPartitionKey.get(i), partitionKey[i]));
            }
        }
        try {
            BoundStatement boundStatement = bindPreparedStatement(delete, clauses);
            result = executeStatement(boundStatement, true, true);
        } catch (ResultAccessException e) {
            result.setError(e);
        }
        return result;
    }

    /**
     * Delete where.
     * <p/>
     * Currently only the =,<, and > operands are supported on primary keys (no secondary indexes).
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p/>
     * @param whereConditions the conditions of the delete.
     * @return the result
     */
    public Result<List<T>> deleteWhere(List<Clause> whereConditions) {
        Delete delete = QueryBuilder.delete().from(keyspaceName + "." + tableName);
        Result<List<T>> result = null;

        try {
            BoundStatement boundStatement = bindPreparedStatement(delete, whereConditions);
            result = executeStatement(boundStatement);
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
     * Gets count.
     *
     * @param statement the statement
     * @return the count
     */
    @Deprecated
    public Result<Long> getCount(BoundStatement statement) {
        if (statement.preparedStatement().getQueryString().contains("count")) {
            ResultSetFuture future = session.executeAsync(statement);

            return getCount(future);
        } else {
            Result result = new Result();
            result.setError(new ResultAccessException(new Exception("Bad count query")));

            return result;
        }
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
        Select select = QueryBuilder.select().countAll().from(keyspaceName + "." + tableName + "").limit(limit);

        if (whereConditions != null && whereConditions.size() > 0) {
            Select.Where where = select.where();

            for (Clause condition : whereConditions) {
                where.and(condition);
            }
        }
        ResultSetFuture future = session.executeAsync(select);

        return getCount(future);
    }

    private Result getCount(ResultSetFuture future) {
        final Result result = new Result();

        Futures.addCallback(future, new FutureCallback() {
            @Override
            public void onSuccess(Object obj) {
                result.setUnmappedResultSet((ResultSet) obj);
                result.setMappedResult(((ResultSet) obj).one().getLong("count"));
            }

            @Override
            public void onFailure(Throwable throwable) {
                result.setError(throwable);
            }
        });

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
        ConsistencyLevel consistencyLevel = cqlUtils.getConsistencyLevel(CQLUtils.QueryType.WRITE);

        for (Statement statement : batch.getStatements()) {
            statement.setConsistencyLevel(consistencyLevel);
        }
        return executeStatement(batch);
    }

    private Result executeStatement(Statement statement) {
        return executeStatement(statement, false, false);
    }

    private Result executeStatement(Statement statement, boolean asListIfOne, boolean mapResult) {
        Result result = new Result();
        ResultSetFuture future = session.executeAsync(statement);

        addCallBack(result, future, asListIfOne, mapResult);

        return result;
    }

    private void addCallBack(final Result result, ListenableFuture future, final Boolean asListIfOne, final boolean mapResult) {
        Futures.addCallback(future, new FutureCallback() {
            @Override
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

            @Override
            public void onFailure(Throwable throwable) {
                result.setError(throwable);
            }
        });
    }

    private void mapResult(Result result, Object obj, Boolean asListIfOne) {
        com.datastax.driver.mapping.Result results = mapper.map((ResultSet) obj);
        List list = results.all();

        if (!asListIfOne && list.size() == 1) {
            result.setMappedResult(list.get(0));
        } else if (list.size() != 0) {
            result.setMappedResult(list);
        } else {
            result.setMappedResult(null);
        }
    }

    private BoundStatement bindPreparedStatement(BuiltStatement statement, List<Clause> whereConditions) throws ResultAccessException {
        String preparedQueryString = statement.getQueryString();
        int hash = preparedQueryString.hashCode();
        PreparedStatement preparedQuery;
        BoundStatement boundPreparedStatement;
        List orderedValues = new ArrayList();

        if (whereConditions != null && whereConditions.size() > 0) {
            List<String> orderedKeys = new ArrayList<String>();
            HashMap whereConditionsMap = new HashMap();
            StringBuffer preparedQueryStringBuffer = new StringBuffer(preparedQueryString.replaceAll(";"," where "));
            Pattern p = Pattern.compile("(\"?\\w+\"?[<>=])'(\\w+)'");
            String column;
            Matcher m = null;
            int i = 0;

            if(statement instanceof Delete) {
                Delete.Where where = ((Delete)statement).where();

                for (Clause condition : whereConditions) {
                    where.and(condition);
                }
                m = p.matcher(where.toString());
            } else if(statement instanceof Select) {
                Select.Where where = ((Select)statement).where();

                for (Clause condition : whereConditions) {
                    where.and(condition);
                }
                m = p.matcher(where.toString());
                m.groupCount();
            }
            while (m.find()) {
                column = m.group(1);
                orderedKeys.add(column);
                whereConditionsMap.put(column, m.group(2));
            }

            for(; i < orderedKeys.size(); i++) {
                column = orderedKeys.get(i);

                if(column.replaceAll(".$", "").equals(orderedPrimaryKey.get(i))) {
                    orderedValues.add(whereConditionsMap.get(column));
                    whereConditionsMap.remove(column);
                    preparedQueryStringBuffer.append(column).append("?").append(" and ");
                } else {
                    throw new ResultAccessException("Your keys are out of order");
                }
            }
            if(!whereConditionsMap.isEmpty()) {
                throw new ResultAccessException("Some of your columns are not part of the primary key");
            }
            preparedQueryString = preparedQueryStringBuffer.toString();
            preparedQueryString = preparedQueryString.substring(0, preparedQueryString.lastIndexOf(" and ")).concat(";");
            hash = preparedQueryString.hashCode();
        }
        synchronized (preparedQueries) {
            if ((preparedQuery = preparedQueries.get(hash)) == null) {
                preparedQuery = session.prepare(preparedQueryString);
                preparedQueries.put(hash, preparedQuery);
            }
        }
        boundPreparedStatement = new BoundStatement(preparedQuery);

        for(int i = 0; i < orderedValues.size(); ++i) {
            Object o = orderedValues.get(i);
            if(o instanceof  Integer) {
                boundPreparedStatement.setInt(i, (Integer) o);
            } else if (o instanceof String) {
                boundPreparedStatement.setString(i, (String) o);
            } else if (o instanceof Float) {
                boundPreparedStatement.setFloat(i, (Float)o);
            } else if (o instanceof Long) {
                boundPreparedStatement.setLong(i, (Long)o);
            }
        }

        return boundPreparedStatement;
    }
}