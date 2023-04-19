# Kafka Connect Join Field Transformer

[![Java CI with Gradle](https://github.com/joker1007/kafka-connect-join-field-transformer/actions/workflows/build.yml/badge.svg)](https://github.com/joker1007/kafka-connect-join-field-transformer/actions/workflows/build.yml)

This is an implementation of a kafka-connect transformer to join multiple fields as a string.

# Configuration

| name        | type   | required | default | description                                               |
|-------------|--------|----------|---------|-----------------------------------------------------------|
| fields      | string | true     |         | fields name you want to join (comma separated)            |
| joined.name | string | true     |         | output field name                                         |
| separator   | string | false    | `"_"`   | separator string                                          |
| format      | string | false    | `null`  | String.format style formatter (precedence over separator) |
