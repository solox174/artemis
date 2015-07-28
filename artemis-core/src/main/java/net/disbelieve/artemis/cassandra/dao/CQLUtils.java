package com.comcast.artemis.cassandra.dao;

import com.comcast.artemis.exception.ResultAccessException;
import com.comcast.artemis.jmx.CassandraMetadataMXBeanImpl;
import com.comcast.artemis.jmx.MXBeansManager;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

/**
 * Created by kmatth207 on 6/3/2015.
 */
public class CQLUtils {
    private List<String> orderedClusterKey;
    private List<String> orderedPartitionKey;
    private List<String> orderedPrimaryKey;
    private String tableName;
    private String keySpaceName;


    static enum QueryType {
        READ,
        WRITE
    }

    CQLUtils(Class modelClass) {
        orderedClusterKey = new ArrayList<String>();
        orderedPartitionKey = new ArrayList<String>();
        orderedPrimaryKey = new ArrayList<String>();
        setKeySpace(modelClass);
        setTable(modelClass);
        setPrimaryKey(modelClass);
    }

    List<Clause> buildClause(List<String> orderedPrimaryKey, Object model) throws ResultAccessException {
        String columnName, columnValue, getterName, fieldName;
        Map<String, String> columns = new HashMap<String, String>();
        List<Clause> clauses = new ArrayList<Clause>();
        List<Annotation> annotations;
        Field[] fields = model.getClass().getDeclaredFields();
        Method method;

        for (Field field : fields) {
            fieldName = field.getName();
            columnName = getColumnName(field);
            annotations = Arrays.asList(field.getDeclaredAnnotations());

            if(!annotations.isEmpty() && (annotations.get(0) instanceof PartitionKey ||
                    annotations.get(0) instanceof ClusteringColumn)) {
                getterName = "get"+fieldName.substring(0,1).toUpperCase()+fieldName.substring(1);

                try {
                    method = model.getClass().getMethod(getterName);
                    columnValue = (String)method.invoke(model);

                    if(columnValue != null) {
                        columns.put(columnName, columnValue);
                    }
                } catch (NoSuchMethodException e) {
                    throw new ResultAccessException(e, "No getter for this field exists.");
                } catch (InvocationTargetException e) {
                    throw new ResultAccessException(e, "Could not call getter for this field.");
                } catch (IllegalAccessException e) {
                    throw new ResultAccessException(e, "Could not call getter for this field.");
                }
            }
        }

        for(String key : orderedPrimaryKey) {
            if(columns.containsKey(key)) {
                clauses.add(QueryBuilder.eq(key, columns.get(key)));
            }
        }

        return clauses;
    }

    String getColumnName(Field columnField) {
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

    /**
     * Gets consistency level.
     *
     * @param queryType the query type
     * @return the consistency level
     */
    static ConsistencyLevel getConsistencyLevel(QueryType queryType) {
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

    String getKeySpace() {
        return this.keySpaceName;
    }

    private void setKeySpace(Class modelClass) {
        if (modelClass != null) {
            Table table = (Table) modelClass.getAnnotation(Table.class);
            this.keySpaceName = table.caseSensitiveKeyspace() ? "\"" + table.keyspace() + "\"" : table.keyspace();
        }
    }

    String getTable() {
       return tableName;
    }

    private void setTable(Class modelClass) {
        if (modelClass != null) {
            Table table = (Table) modelClass.getAnnotation(Table.class);
            this.tableName = table.caseSensitiveTable() ? "\"" + table.name() + "\"" : table.name();
        }
    }

    List<String> getPrimaryKey() {
        return this.orderedPrimaryKey;
    }

    private void setPrimaryKey(Class modelClass) {
        setPartitionKey(modelClass);
        setClusterKey(modelClass);
        this.orderedPrimaryKey.addAll(getPartitionKey());
        this.orderedPrimaryKey.addAll(getClusterKey());
    }

    List<String> getPartitionKey() {
        return this.orderedPartitionKey;
    }

    private void setPartitionKey(Class modelClass) {
        if(modelClass != null) {
            Field[] fields = modelClass.getDeclaredFields();
            PartitionKey partitionKey;
            String columnName;

            for (Field field : fields) {
                if ((partitionKey = field.getAnnotation(PartitionKey.class)) != null) {
                    columnName = getColumnName(field);
                    this.orderedPartitionKey.add(partitionKey.value(), columnName);
                }
            }
        }
    }

    List<String> getClusterKey() {
        return this.orderedClusterKey;
    }

    private void setClusterKey(Class modelClass) {
        if(modelClass != null) {
            Field[] fields = modelClass.getDeclaredFields();
            ClusteringColumn clusteringColumn;
            String columnName;

            for (Field field : fields) {
                if ((clusteringColumn = field.getAnnotation(ClusteringColumn.class)) != null) {
                    columnName = getColumnName(field);
                    this.orderedClusterKey.add(clusteringColumn.value(), columnName);
                }
            }
        }
    }
}
