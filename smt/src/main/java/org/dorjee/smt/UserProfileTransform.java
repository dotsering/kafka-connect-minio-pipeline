package org.dorjee.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.time.Instant;
import java.util.Map;

public class UserProfileTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private Schema outputSchema;

    @Override
    public void configure(Map<String, ?> configs) {
        // No config needed
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) return record;

        Struct originalStruct = (Struct) record.value();
        Schema originalSchema = record.valueSchema();

        // 1. FILTERING LOGIC
        String fName = originalStruct.getString("first_name");
        String lName = originalStruct.getString("last_name");

        // If names are missing, return null to drop the record
        if (fName == null || fName.trim().isEmpty() || lName == null || lName.trim().isEmpty()) {
            return null;
        }

        // 2. SCHEMA EVOLUTION
        if (outputSchema == null) {
            SchemaBuilder builder = SchemaBuilder.struct();
            // Copy fields EXCEPT dropped ones
            for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
                if (!field.name().equals("raw_password_hash") &&
                        !field.name().equals("internal_tracking_code")) {
                    builder.field(field.name(), field.schema());
                }
            }
            // Add Derived Columns
            builder.field("full_name", Schema.STRING_SCHEMA);
            builder.field("is_adult", Schema.BOOLEAN_SCHEMA);
            builder.field("processed_ts", Schema.STRING_SCHEMA);
            outputSchema = builder.build();
        }

        // 3. DATA TRANSFORMATION
        Struct newStruct = new Struct(outputSchema);

        for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
            if (!field.name().equals("raw_password_hash") &&
                    !field.name().equals("internal_tracking_code")) {
                newStruct.put(field.name(), originalStruct.get(field.name()));
            }
        }

        // Derived calculations
        newStruct.put("full_name", fName + " " + lName);
        Integer age = originalStruct.getInt32("age");
        newStruct.put("is_adult", age != null && age >= 18);
        newStruct.put("processed_ts", Instant.now().toString());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                outputSchema,
                newStruct,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() { }
}