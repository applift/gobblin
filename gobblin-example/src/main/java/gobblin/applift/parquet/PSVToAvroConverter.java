package gobblin.applift.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.gson.JsonElement;

import gobblin.applift.simpleconsumer.PSVToJSONConverter;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.ToAvroConverterBase;

public class PSVToAvroConverter extends ToAvroConverterBase<Schema, String> {
	private static final Logger LOG = LoggerFactory.getLogger(PSVToAvroConverter.class);
	private final static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Schema convertSchema(Schema schema, WorkUnitState workUnit) throws SchemaConversionException {
		return schema;
	}

	@Override
	public Iterable<GenericRecord> convertRecord(Schema outputSchema, String inputRecord, WorkUnitState workUnit)
			throws DataConversionException {
		if (inputRecord.contains("LOGROTATE"))
			return new EmptyIterable<GenericRecord>();
		String[] psvRecords = inputRecord.split("\n");
		List<GenericRecord> avroRecords = new ArrayList<GenericRecord>();
		for (String psvRecord : psvRecords) {
			String jsonRecord = null;
			try {
				GenericRecord record;
				jsonRecord = PSVToJSONConverter.psvToJson(psvRecord);
				record = JsonToAvroConverter.convert(mapper.readValue(jsonRecord, Map.class), outputSchema);
				avroRecords.add(record);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				LOG.warn("Applift: Faulty Record: "+ jsonRecord);
				e.printStackTrace();
			}
		}
		return avroRecords;
	}

}
