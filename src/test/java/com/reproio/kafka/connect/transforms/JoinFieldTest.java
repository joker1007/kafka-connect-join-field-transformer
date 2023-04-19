package com.reproio.kafka.connect.transforms;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class JoinFieldTest {
  private Map<String, String> buildConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("fields", "key1,key2");
    config.put("joined.name", "joined");

    return config;
  }

  @Test
  void testApplySchemaless() {
    JoinField<SinkRecord> transformer = new JoinField.Value<>();

    Map<String, Object> value = new HashMap<>();
    value.put("key1", 1);
    value.put("key2", "str");

    var record = new SinkRecord("test-topic", 1, null, "key", null, value, 1);
    var config = buildConfig();

    transformer.configure(config);

    var result = transformer.apply(record).value();

    assertEquals(Map.of("key1", 1, "key2", "str", "joined", "1_str"), result);
  }

  @Test
  void testApplySchemalessWithFormat() {
    JoinField<SinkRecord> transformer = new JoinField.Value<>();

    Map<String, Object> value = new HashMap<>();
    value.put("key1", 1);
    value.put("key2", "str");

    var record = new SinkRecord("test-topic", 1, null, "key", null, value, 1);
    var config = buildConfig();
    config.put("format", "%010d_%s");

    transformer.configure(config);

    var result = transformer.apply(record).value();

    assertEquals(Map.of("key1", 1, "key2", "str", "joined", "0000000001_str"), result);
  }

  @Test
  void testApplyWithSchema() {
    JoinField<SinkRecord> transformer = new JoinField.Value<>();

    var schema =
        SchemaBuilder.struct()
            .field("key1", Schema.INT32_SCHEMA)
            .field("key2", Schema.STRING_SCHEMA)
            .build();

    Struct value = new Struct(schema);
    value.put("key1", 1);
    value.put("key2", "str");

    var record = new SinkRecord("test-topic", 1, null, "key", schema, value, 1);
    var config = buildConfig();

    transformer.configure(config);

    var result = (Struct) transformer.apply(record).value();

    assertEquals(1, result.get("key1"));
    assertEquals("str", result.get("key2"));
    assertEquals("1_str", result.get("joined"));
  }

  @Test
  void testApplyWithSchemaWithFormat() {
    JoinField<SinkRecord> transformer = new JoinField.Value<>();

    var schema =
        SchemaBuilder.struct()
            .field("key1", Schema.INT32_SCHEMA)
            .field("key2", Schema.STRING_SCHEMA)
            .build();

    Struct value = new Struct(schema);
    value.put("key1", 1);
    value.put("key2", "str");

    var record = new SinkRecord("test-topic", 1, null, "key", schema, value, 1);
    var config = buildConfig();
    config.put("format", "%010d_%s");

    transformer.configure(config);

    var result = (Struct) transformer.apply(record).value();

    assertEquals(1, result.get("key1"));
    assertEquals("str", result.get("key2"));
    assertEquals("0000000001_str", result.get("joined"));
  }
}
