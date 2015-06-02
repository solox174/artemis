package net.disbelieve.artemis.cassandra.dao;

import net.disbelieve.artemis.cassandra.CassandraConnect;
import net.disbelieve.artemis.exception.ResultAccessException;
import net.disbelieve.artemis.jmx.CassandraMetadataMXBeanImpl;
import net.disbelieve.artemis.jmx.MXBeansManager;
import net.disbelieve.artemis.utils.Result;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.lang.reflect.Field;
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
    /**
     * The Mapping manager.
     */
    protected MappingManager mappingManager;
    /**
     * The Mapper.
     */
    protected Mapper mapper;
    /**
     * The Keyspace name.
     */
    protected String keyspaceName;
    /**
     * The Table name.
     */
    protected String tableName;

    private enum QueryType {
        /**
         * The READ.
         */
        READ,
        /**
         * The WRITE.
         */
        WRITE
    }

    /**
     * The Session.
     */
    protected static Session session;
    private Map<Integer, PreparedStatement> preparedQueries;
    private ArrayList<String> orderedPartitionKey;
    private ArrayList<String> orderedClusteringColumn;

    /**
     * Instantiates a new Cassandra DAO.
     */
    public CassandraDAO() {
        Type type = this.getClass().getGenericSuperclass();

        session = CassandraConnect.getSession();
        mappingManager = new MappingManager(session);
        preparedQueries = new HashMap<Integer, PreparedStatement>();
        orderedPartitionKey = new ArrayList<String>();
        orderedClusteringColumn = new ArrayList<String>();

        if (type instanceof ParameterizedType) {
            Type[] types = ((ParameterizedType) type).getActualTypeArguments();
            Class modelClass = (Class) types[0];
            Field[] fields = modelClass.getDeclaredFields();
            Table table = (Table) modelClass.getAnnotation(Table.class);
            PartitionKey partitionKey;
            ClusteringColumn clusteringColumn;
            String columnName;
            keyspaceName = table.caseSensitiveKeyspace() ? "\"" + table.keyspace() + "\"" : table.keyspace();
            tableName = table.caseSensitiveTable() ? "\"" + table.name() + "\"" : table.name();
            mapper = mappingManager.mapper(modelClass);

            for (Field field : fields) {
                if ((partitionKey = field.getAnnotation(PartitionKey.class)) != null) {
                    columnName = getColumnName(field);
                    orderedPartitionKey.add(partitionKey.value(), columnName);
                } else if ((clusteringColumn = field.getAnnotation(ClusteringColumn.class)) != null) {
                    columnName = getColumnName(field);
                    orderedClusteringColumn.add(clusteringColumn.value(), columnName);
                }
            }
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
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.READ));

        return executeStatement(statement, false, true);
    }

    /**
     * Gets all records by partition key.
     *
     * @param getAllStatement the preparedStatement defining "all" for the model type
     * @return the Result containing the ResultSet mapped to the model type and, in the case of an error,
     * the Throwable
     */
    @Deprecated
    public Result<List<T>> getAll(BoundStatement getAllStatement) {
        getAllStatement.setConsistencyLevel(getConsistencyLevel(QueryType.READ));

        return executeStatement(getAllStatement, true, true);
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
     * Inserts/updates a single record into Cassandra
     *
     * @param model the model to be written into Cassandra
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    public Result putOne(Object model) {
        Statement statement = mapper.saveQuery(model);
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));

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
        statement.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));

        return executeStatement(statement);
    }

    /**
     * Deletes all records in a row
     *
     * @param deleteAll the preparedStatement defining "all" for the model type
     * @return the Result containing the ResultSet and, in the case of an error,
     * the Throwable
     */
    @Deprecated
    public Result<List<T>> deleteAll(BoundStatement deleteAll) {
        deleteAll.setConsistencyLevel(getConsistencyLevel(QueryType.WRITE));

        return executeStatement(deleteAll);
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
        ConsistencyLevel consistencyLevel = getConsistencyLevel(QueryType.WRITE);

        for (Statement statement : batch.getStatements()) {
            statement.setConsistencyLevel(consistencyLevel);
        }
        return executeStatement(batch);
    }

    /**
     * Execute statement. Does not map.
     *
     * @param statement the statement to execute
     * @return the result
     */
    protected Result executeStatement(Statement statement) {
        return executeStatement(statement, false, false);
    }

    /**
     * Execute statement.
     *
     * @param statement   the statement to execute
     * @param asListIfOne mappedResult as a list even for one Row
     * @param mapResult   map the result
     * @return the result
     */
    protected Result executeStatement(Statement statement, boolean asListIfOne, boolean mapResult) {
        Result result = new Result();
        ResultSetFuture future = session.executeAsync(statement);

        addCallBack(result, future, asListIfOne, mapResult);

        return result;
    }

    /**
     * Add call back.
     *
     * @param result      the result
     * @param future      the future
     * @param asListIfOne the as list if one
     * @param mapResult   the map result
     */
    protected void addCallBack(final Result result, ListenableFuture future, final Boolean asListIfOne, final boolean mapResult) {
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

    /**
     * Map result.
     *
     * @param result      the result
     * @param obj         the obj
     * @param asListIfOne the as list if one
     */
    protected void mapResult(Result result, Object obj, Boolean asListIfOne) {
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

    /**
     * Gets consistency level.
     *
     * @param queryType the query type
     * @return the consistency level
     */
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

    private BoundStatement bindPreparedStatement(BuiltStatement statement, List<Clause> whereConditions) throws ResultAccessException {
        String preparedQueryString = statement.getQueryString();
        int hash = preparedQueryString.hashCode();
        PreparedStatement preparedQuery;
        BoundStatement boundPreparedStatement;
        List<String> orderedValues = new ArrayList<String>();

        if (whereConditions != null && whereConditions.size() > 0) {
            List<String> orderedPrimaryKey = new ArrayList<String>();
            List<String> orderedKeys = new ArrayList<String>();
            HashMap<String, String> whereConditionsMap = new HashMap<String, String>();
            StringBuffer preparedQueryStringBuffer = new StringBuffer(preparedQueryString.replaceAll(";"," where "));
            Pattern p = Pattern.compile("(\"?\\w+\"?[<>=])'(\\w+)'");
            String column;
            Matcher m = null;
            int i = 0;
            orderedPrimaryKey.addAll(orderedPartitionKey);
            orderedPrimaryKey.addAll(orderedClusteringColumn);

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

        for(String orderedValue : orderedValues) {
            boundPreparedStatement.bind(orderedValue);
        }

        return boundPreparedStatement;
    }

    private String getColumnName(Field columnField) {
        Column column;
        String columnName;

        if((column = columnField.getAnnotation(Column.class)) != null) {
            columnName = column.name();

            if(column.caseSensitive()) {
                columnName = QueryBuilder.quote(columnName);
            }
        } else {
            columnName = columnField.getName().split("get")[0];
        }

        return columnName;
    }

    /*PreparedStatement getPreparedQuery(com.datastax.driver.mapping.QueryType type) {
        PreparedStatement stmt = preparedQueries.get(type);
        if (stmt == null) {
            synchronized (preparedQueries) {
                stmt = preparedQueries.get(type);
                if (stmt == null) {
                    String query = type.makePreparedQueryString(tableMetadata, mapper);
                    logger.debug("Preparing query {}", query);
                    stmt = session().prepare(query);
                    Map<com.datastax.driver.mapping.QueryType, PreparedStatement> newQueries = new HashMap<com.datastax.driver.mapping.QueryType, PreparedStatement>(preparedQueries);
                    newQueries.put(type, stmt);
                    preparedQueries = newQueries;
                }
            }
        }
        return stmt;
    }*/
}