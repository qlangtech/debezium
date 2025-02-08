/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.fest.assertions.Assertions;
import org.junit.Test;
import com.kingbase8.util.KSQLException;
import com.kingbase8.util.KSQLState;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

public class PostgresErrorHandlerTest {
    private static final String A_CLASSIFIED_EXCEPTION = "Database connection failed when writing to copy";

    private final PostgresErrorHandler errorHandler = new PostgresErrorHandler(
            new PostgresConnectorConfig(Configuration.create()
                    .with(PostgresConnectorConfig.SERVER_NAME, "postgres")
                    .build()),
            new ChangeEventQueue.Builder<DataChangeEvent>().build());

    @Test
    public void classifiedKSQLExceptionIsRetryable() {
        KSQLException testException = new KSQLException(A_CLASSIFIED_EXCEPTION, KSQLState.CONNECTION_FAILURE);
        Assertions.assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    public void KSQLExceptionWithNullErrorMesdsageNotRetryable() {
        KSQLException testException = new KSQLException(null, KSQLState.CONNECTION_FAILURE);
        Assertions.assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    @Test
    public void nullThrowableIsNotRetryable() {
        Assertions.assertThat(errorHandler.isRetriable(null)).isFalse();
    }

    @Test
    public void unclassifiedKSQLExceptionIsNotRetryable() {
        KSQLException testException = new KSQLException(
                "definitely not a postgres error", KSQLState.CONNECTION_FAILURE);
        Assertions.assertThat(errorHandler.isRetriable(testException)).isFalse();
    }

    @Test
    public void classifiedKSQLExceptionWrappedInDebeziumExceptionIsRetryable() {
        KSQLException KSQLException = new KSQLException(A_CLASSIFIED_EXCEPTION, KSQLState.CONNECTION_FAILURE);
        DebeziumException testException = new DebeziumException(KSQLException);
        Assertions.assertThat(errorHandler.isRetriable(testException)).isTrue();
    }

    @Test
    public void randomUnhandledExceptionIsNotRetryable() {
        RuntimeException testException = new RuntimeException();
        Assertions.assertThat(errorHandler.isRetriable(testException)).isFalse();
    }
}
