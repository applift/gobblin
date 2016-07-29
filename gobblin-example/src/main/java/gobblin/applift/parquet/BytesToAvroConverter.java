package gobblin.applift.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.ToAvroConverterBase;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class BytesToAvroConverter extends ToAvroConverterBase<Schema, byte[]> {

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    List<GenericRecord> avroRecords = new ArrayList<GenericRecord>();
    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(outputSchema);
    GenericRecord record = recordInjection.invert(inputRecord).get();
    avroRecords.add(record);
    return avroRecords;
  }
}
