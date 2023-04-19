package com.reproio.kafka.connect.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class JoinField<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String FIELDS_CONFIG = "fields";
  private static final String JOINED_NAME_CONFIG = "joined.name";
  private static final String SEPARATOR_CONFIG = "separator";
  private static final String FORMAT_CONFIG = "format";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              FIELDS_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              Importance.MEDIUM,
              "Fields name to join.")
          .define(
              JOINED_NAME_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              Importance.MEDIUM,
              "Output field name")
          .define(SEPARATOR_CONFIG, Type.STRING, "_", Importance.MEDIUM, "Separator string")
          .define(FORMAT_CONFIG, Type.STRING, null, Importance.MEDIUM, "String Format");

  private static final String PURPOSE = "fields extraction and join them as a string";

  private List<String> fieldsName;
  private String joinedName;
  private String separator;
  private String format;

  private SynchronizedCache<Schema, Schema> schemaUpdateCache;

  protected JoinField() {}

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldsName =
        Arrays.stream(config.getString(FIELDS_CONFIG).split(",")).collect(Collectors.toList());
    joinedName = config.getString(JOINED_NAME_CONFIG);
    separator = config.getString(SEPARATOR_CONFIG);
    format = config.getString(FORMAT_CONFIG);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  @Override
  public R apply(R record) {
    if (operatingValue(record) == null) {
      return record;
    }
    final Schema schema = operatingSchema(record);
    if (schema == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record, schema);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
    final Map<String, Object> updatedValue = new HashMap<>(value);

    String joined;
    if (format == null) {
      joined =
          fieldsName.stream()
              .map(value::get)
              .filter(Objects::nonNull)
              .map(Object::toString)
              .collect(Collectors.joining(separator));
    } else {
      Object[] fieldValues = fieldsName.stream().map(value::get).filter(Objects::nonNull).toArray();
      joined = String.format(format, fieldValues);
    }
    if (!joined.isEmpty()) {
      updatedValue.put(joinedName, joined);
      return newRecord(record, null, updatedValue);
    } else {
      return record;
    }
  }

  private Stream<Object> getNonNullFieldValues(Struct struct, Schema schema) {
    return fieldsName.stream()
        .map(
            name -> {
              var f = schema.field(name);
              if (f == null) {
                throw new IllegalArgumentException("Unknown field: " + name);
              }
              return struct.get(f);
            })
        .filter(Objects::nonNull);
  }

  private R applyWithSchema(R record, Schema schema) {
    final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
    Schema updatedSchema = schemaUpdateCache.get(schema);
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(schema);
      schemaUpdateCache.put(schema, updatedSchema);
    }

    Struct updatedValue = new Struct(updatedSchema);

    String joined;
    if (format == null) {
      joined =
          getNonNullFieldValues(value, schema)
              .map(Object::toString)
              .collect(Collectors.joining(separator));
    } else {
      Object[] fieldValues = getNonNullFieldValues(value, schema).toArray();
      joined = String.format(format, fieldValues);
    }

    for (Field field : value.schema().fields()) {
      updatedValue.put(field.name(), value.get(field));
    }

    updatedValue.put(joinedName, joined);

    return newRecord(record, updatedSchema, updatedValue);
  }

  private Schema makeUpdatedSchema(Schema schema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(joinedName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  @Override
  public void close() {}

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends JoinField<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          updatedSchema,
          updatedValue,
          record.valueSchema(),
          record.value(),
          record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends JoinField<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          updatedSchema,
          updatedValue,
          record.timestamp());
    }
  }
}
