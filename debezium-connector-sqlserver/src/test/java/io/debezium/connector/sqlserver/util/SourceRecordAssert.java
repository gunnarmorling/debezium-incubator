/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.util;

import io.debezium.data.VerifyRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;

public class SourceRecordAssert {

    public static SourceRecordAssert assertThat(SourceRecord sourceRecord) {
        return new SourceRecordAssert(sourceRecord);
    }

    private final SourceRecord record;

    private SourceRecordAssert(SourceRecord record) {
        this.record = record;
    }

    public SourceRecordAssert valueAfterFieldIsEqualTo(Struct expectedValue) {
        Struct value = (Struct) record.value();
        Struct afterValue = (Struct) value.get("after");
        Assertions.assertThat(afterValue).isEqualTo(expectedValue);
        return this;
    }

    public SourceRecordAssert valueAfterFieldSchemaIsEqualTo(Schema expectedSchema) {
        Schema valueSchema = record.valueSchema();
        Schema afterFieldSchema = valueSchema.field("after").schema();
        VerifyRecord.assertConnectSchemasAreEqual(null, afterFieldSchema, expectedSchema);
        return this;
    }
}
