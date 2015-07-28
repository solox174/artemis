package com.comcast.artemis.test.dao;

import com.comcast.artemis.cassandra.dao.CassandraDAO;
import com.comcast.artemis.cassandra.dao.Repository;
import com.comcast.artemis.test.data.TestLongCompoundKey;
import com.comcast.artemis.test.data.TestStringSimpleKey;

/**
 * Created by kmatth207 on 5/22/2015.
 *
 */
@Repository
public class TestLongCompoundDAO extends CassandraDAO<TestLongCompoundKey> {
}
