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
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * @author ynli
 */
@SuppressWarnings("unused")
public class JsonToAvroConverter extends ToAvroConverterBase<Schema, String> {

  private static final Gson GSON = new Gson();
  // Expect the input JSON string to be key-value pairs
  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {
  }.getType();

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
  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, String jsonRecords, WorkUnitState workUnit)
      throws DataConversionException {
    if (jsonRecords.contains("LOGROTATE"))
      return new EmptyIterable<GenericRecord>();
    String[] inputRecords = jsonRecords.split("\n");
    List<GenericRecord> avroRecords = new ArrayList<GenericRecord>();
    for (String inputRecord : inputRecords) {
      String normalizedLog = LogNormalizerConverter.normalizeString(inputRecord);
      GenericRecord record = new GenericData.Record(schema);
      JsonElement element = GSON.fromJson(normalizedLog, JsonElement.class);
      Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
      for (Map.Entry<String, Object> entry : fields.entrySet()) {
        record.put(entry.getKey(), entry.getValue());
      }
      avroRecords.add(record);
    }
    return avroRecords;
  }
  
  private static void jsonToAvroRecord(JsonElement elem, GenericRecord record){
    
    
  }
}
