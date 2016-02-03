package io.bifroest.aggregator.systems.cassandra;

import java.time.Duration;
import java.util.Iterator;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.bifroest.commons.model.Metric;
import io.bifroest.retentions.RetentionTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WrappedCassandraSession implements CassandraSession {
    private static final Logger log = LogManager.getLogger();
    private final Session session;
    private final Duration waitAfterWriteTimeout;

    public WrappedCassandraSession(Session session, Duration waitAfterWriteTimeout) {
        this.session = session;
        this.waitAfterWriteTimeout = waitAfterWriteTimeout;
    }

    @Override
    public void dropTableInDatabase(RetentionTable table) {
        session.execute("DROP TABLE " + table.tableName() + ";");
    }

    @Override
    public Iterator<Row> loadMetricsFromTable(RetentionTable table, String name) {
        Clause cName = QueryBuilder.eq(CassandraAccessLayer.COL_NAME, name);
        Statement stm = QueryBuilder.select().all().from(table.tableName()).where(cName);
        return session.execute(stm).iterator();
    }

    @Override
    public void createTable(RetentionTable table) {
        StringBuilder query = new StringBuilder();
        query.append("CREATE TABLE IF NOT EXISTS ").append(table.tableName()).append(" (");
        query.append(CassandraAccessLayer.COL_NAME).append(" text, ");
        query.append(CassandraAccessLayer.COL_TIME).append(" bigint, ");
        query.append(CassandraAccessLayer.COL_VALUE).append(" double, ");
        query.append("PRIMARY KEY (").append(CassandraAccessLayer.COL_NAME).append(", ").append(CassandraAccessLayer.COL_TIME).append(")");
        query.append(");");
        session.execute(query.toString());
    }

    @Override
    public Iterator<Row> loadNamesFromTable(RetentionTable table) {
        Statement stm = QueryBuilder.select().distinct().column(CassandraAccessLayer.COL_NAME).from(table.tableName());
        return session.execute(stm).iterator();
    }

    @Override
    public void insertMetric(RetentionTable table, Metric metric) {
        String[] columns = {CassandraAccessLayer.COL_NAME, CassandraAccessLayer.COL_TIME, CassandraAccessLayer.COL_VALUE};
        Object[] values = {metric.name(), metric.timestamp(), metric.value()};
        Statement stm = QueryBuilder.insertInto(table.tableName()).values(columns, values);

        try {
            session.execute(stm);
        } catch (WriteTimeoutException e) {
            log.info("WriteTimeoutException while sending Metrics to cassandra.");
            log.info(e.getMessage());
            log.info("According to http://www.datastax.com/dev/blog/how-cassandra-deals-with-replica-failure, this is harmless");

            try {
                Thread.sleep(waitAfterWriteTimeout.toMillis());
            } catch (InterruptedException e1) {
                // ignore
            }
        }
    }

    @Override
    public void close() {
        session.close();
    }
}