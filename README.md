# Kafka Connect Join Field Transformer

This is an implementation of a kafka-connect transformer to join multiple fields as a string.

# Configuration

| name        | type   | required | default | description                                               |
|-------------|--------|----------|---------|-----------------------------------------------------------|
| fields      | string | true     |         | fields name you want to join (comma separated)            |
| joined.name | string | true     |         | output field name                                         |
| separator   | string | false    | `"_"`   | separator string                                          |
| format      | string | false    | `null`  | String.format style formatter (precedence over separator) |
