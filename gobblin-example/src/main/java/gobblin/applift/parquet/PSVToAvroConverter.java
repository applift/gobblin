package gobblin.applift.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.google.common.base.Splitter;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.ToAvroConverterBase;

public class PSVToAvroConverter extends ToAvroConverterBase<Schema, String> {

  @Override
  public Schema convertSchema(Schema schema, WorkUnitState workUnit) throws SchemaConversionException {
    return schema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, String inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    if(inputRecord.contains("LOGROTATE"))
      return new EmptyIterable<GenericRecord>();
    List<String> inputPSVRecords = Splitter.on("\n").trimResults().omitEmptyStrings().splitToList(inputRecord);
    List<GenericRecord> avroRecords = new ArrayList<GenericRecord>();
    for(String inputPSVRecord:inputPSVRecords){
      GenericRecord record = new GenericRecordBuilder(outputSchema).build();
      List<String> inputFields = Splitter.on("|").trimResults().omitEmptyStrings().splitToList(inputRecord);
      int i = 0;
      for (String inputField : inputFields) {
        record.put(i, inputField);
      }
      avroRecords.add(record);
    }
    return avroRecords;
  }

}
