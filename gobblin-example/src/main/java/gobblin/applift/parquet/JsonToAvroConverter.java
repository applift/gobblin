/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.applift.parquet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import gobblin.applift.simpleconsumer.LogNormalizerConverter;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.converter.ToAvroConverterBase;

/**
 * An implementation of {@link Converter} for the simple JSON example.
 *
 * <p>
 * This converter converts the input string schema into an Avro
 * {@link org.apache.avro.Schema} and each input json document into an Avro
 * {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 *
 * @author prashant.bhardwaj@applift.com
 */
@SuppressWarnings("unused")
public class JsonToAvroConverter extends ToAvroConverterBase<Schema, String> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonToAvroConverter.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /*
   * For converting bulk Json Records each per line to Avro GenericRecord
   * 
   * @see
   * gobblin.converter.ToAvroConverterBase#convertRecord(org.apache.avro.Schema,
   * java.lang.Object, gobblin.configuration.WorkUnitState)
   */
  @SuppressWarnings("unchecked")
  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, String jsonRecords, WorkUnitState workUnit)
      throws DataConversionException {
    if (jsonRecords.contains("LOGROTATE"))
      return new EmptyIterable<GenericRecord>();
    String[] inputRecords = jsonRecords.split("\n");
    List<GenericRecord> avroRecords = new ArrayList<GenericRecord>();
    for (String inputRecord : inputRecords) {
      String normalizedLog = LogNormalizerConverter.normalizeString(inputRecord);
      GenericRecord record;
      try {
        record = convert(mapper.readValue(normalizedLog, Map.class),schema);
        avroRecords.add(record);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.warn("Applift: Faulty Record: "+normalizedLog);
      }
    }
    return avroRecords;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private GenericRecord convert(Map<String, Object> raw, Schema schema) throws IOException {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      String name = f.name();
      Object rawValue = raw.get(name);
      if (rawValue != null) {
        result.put(f.pos(), typeConvert(rawValue, name, f.schema()));
      } else {
        JsonNode defaultValue = f.defaultValue();
        if (defaultValue == null || defaultValue.isNull()) {
          if (isNullableSchema(f.schema())) {
            result.put(f.pos(), null);
          } else {
            throw new IllegalArgumentException("No default value provided for non-nullable field: " + f.name());
          }
        } else {
          Schema fieldSchema = f.schema();
          if (isNullableSchema(fieldSchema)) {
            fieldSchema = getNonNull(fieldSchema);
          }
          Object value = null;
          switch (fieldSchema.getType()) {
          case BOOLEAN:
            value = defaultValue.getValueAsBoolean();
            break;
          case DOUBLE:
            value = defaultValue.getValueAsDouble();
            break;
          case FLOAT:
            value = (float) defaultValue.getValueAsDouble();
            break;
          case INT:
            value = defaultValue.getValueAsInt();
            break;
          case LONG:
            value = defaultValue.getValueAsLong();
            break;
          case STRING:
            value = defaultValue.getValueAsText();
            break;
          case MAP:
            Map<String, Object> fieldMap = mapper.readValue(defaultValue.getValueAsText(), Map.class);
            Map<String, Object> mvalue = Maps.newHashMap();
            for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
              mvalue.put(e.getKey(), typeConvert(e.getValue(), name, fieldSchema.getValueType()));
            }
            value = mvalue;
            break;
          case ARRAY:
            List fieldArray = mapper.readValue(defaultValue.getValueAsText(), List.class);
            List lvalue = Lists.newArrayList();
            for (Object elem : fieldArray) {
              lvalue.add(typeConvert(elem, name, fieldSchema.getElementType()));
            }
            value = lvalue;
            break;
          case RECORD:
            Map<String, Object> fieldRec = mapper.readValue(defaultValue.getValueAsText(), Map.class);
            value = convert(fieldRec, fieldSchema);
            break;
          default:
            throw new IllegalArgumentException("JsonConverter cannot handle type: " + fieldSchema.getType());
          }
          result.put(f.pos(), value);
        }
      }
    }
    return result;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Object typeConvert(Object value, String name, Schema schema) throws IOException {
    if (isNullableSchema(schema)) {
      if (value == null) {
        return null;
      } else {
        schema = getNonNull(schema);
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw new JsonConversionException(null, name, schema);
    }

    switch (schema.getType()) {
    case BOOLEAN:
      if (value instanceof Boolean) {
        return (Boolean) value;
      } else if (value instanceof String) {
        return Boolean.valueOf((String) value);
      } else if (value instanceof Number) {
        return ((Number) value).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
      }
      break;
    case DOUBLE:
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      } else if (value instanceof String) {
        return Double.valueOf((String) value);
      }
      break;
    case FLOAT:
      if (value instanceof Number) {
        return ((Number) value).floatValue();
      } else if (value instanceof String) {
        return Float.valueOf((String) value);
      }
      break;
    case INT:
      if (value instanceof Number) {
        return ((Number) value).intValue();
      } else if (value instanceof String) {
        return Integer.valueOf((String) value);
      }
      break;
    case LONG:
      if (value instanceof Number) {
        return ((Number) value).longValue();
      } else if (value instanceof String) {
        return Long.valueOf((String) value);
      }
      break;
    case STRING:
      return value.toString();
    case ENUM:
      try {
        Class<Enum> enumType = (Class<Enum>) Class.forName(schema.getFullName());
        return Enum.valueOf(enumType, value.toString());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    case RECORD:
      return convert((Map<String, Object>) value, schema);
    case ARRAY:
      Schema elementSchema = schema.getElementType();
      List listRes = new ArrayList();
      for (Object v : (List) value) {
        listRes.add(typeConvert(v, name, elementSchema));
      }
      return listRes;
    case MAP:
      Schema valueSchema = schema.getValueType();
      Map<String, Object> mapRes = new HashMap<String, Object>();
      for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
        mapRes.put(v.getKey(), typeConvert(v.getValue(), name, valueSchema));
      }
      return mapRes;
    default:
      throw new IllegalArgumentException("JsonToAvroConverter cannot handle type: " + schema.getType());
    }
    throw new JsonConversionException(value, name, schema);
  }

  private boolean isNullableSchema(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION) && schema.getTypes().size() == 2
        && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
            || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  private Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  private static class JsonConversionException extends RuntimeException {
    private static final long serialVersionUID = 257077172187L;
    private Object value;
    private String fieldName;
    private Schema schema;

    public JsonConversionException(Object value, String fieldName, Schema schema) {
      this.value = value;
      this.fieldName = fieldName;
      this.schema = schema;
    }

    @Override
    public String toString() {
      return String.format("Type conversion error for field %s, %s for %s", fieldName, value, schema);
    }
  }
}
