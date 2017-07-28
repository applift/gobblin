package gobblin.applift.simpleconsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

public class PSVToJSONConverter extends Converter<Object, Object, String, String> {
	private static final Logger LOG = LoggerFactory.getLogger(PSVToJSONConverter.class);
	private static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<String> convertRecord(Object outputSchema, String inputRecord, WorkUnitState workUnit)
			throws DataConversionException {
		if (inputRecord.contains("LOGROTATE"))
			return new SingleRecordIterable<String>(inputRecord);
		String[] psvRecords = inputRecord.split("\n");
		List<String> jsonRecords = new ArrayList<String>();
		for (String psvRecord : psvRecords) {
			try {
				jsonRecords.add(psvToJson(psvRecord));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.error("Erroneous Record : " + psvRecord);
				e.printStackTrace();
			}
		}
		return jsonRecords;
	}

	public static String psvToJson(String psvRecord) throws IOException {
		String[] fields = psvRecord.split("\\|");
		ClickHouseRecord record = new ClickHouseRecord();
		if (!fields[0].isEmpty())
			record.setExchange_id(Integer.parseInt(fields[0]));
		else
			record.setExchange_id(0);
		record.setPublisher_id(fields[1]);
		record.setPublisher_name(fields[2]);
		record.setBundle_id(fields[3]);
		record.setApp_id(fields[4]);
		record.setApp_name(fields[5]);
		if (!fields[6].isEmpty())
			record.setCountry_id(Integer.parseInt(fields[6]));
		if (!fields[7].isEmpty())
			record.setCarrier_id(Integer.parseInt(fields[7]));
		if (!fields[8].isEmpty())
			record.setConnection_type(Integer.parseInt(fields[8]));
		if (!fields[9].isEmpty())
			record.setTraffic_type(Integer.parseInt(fields[9]));
		if (!fields[10].isEmpty())
			record.setOs(Integer.parseInt(fields[10]));
		if (!fields[11].isEmpty())
			record.setOsv(Integer.parseInt(fields[11]));
		if (!fields[12].isEmpty())
			record.setMake_id(Integer.parseInt(fields[12]));
		if (!fields[13].isEmpty())
			record.setHas_location(Integer.parseInt(fields[13]));
		if (!fields[14].isEmpty())
			record.setHas_deviceid(Integer.parseInt(fields[14]));
		if (!fields[15].isEmpty())
			record.setAge(Integer.parseInt(fields[15]));
		if (!fields[16].isEmpty())
			record.setGender(Integer.parseInt(fields[16]));
		if (!fields[17].isEmpty())
			record.setImpression_type(Integer.parseInt(fields[17]));
		if (!fields[18].isEmpty())
			record.setHas_bidfloor(Integer.parseInt(fields[18]));
		if (!fields[19].isEmpty())
			record.setHour(Integer.parseInt(fields[19]));
		if (!fields[20].isEmpty())
			record.setDate(fields[20]);
		if (!fields[21].isEmpty())
			record.setBid_floor(Double.parseDouble(fields[21]));
		record.setDevice_id(getDeviceId(fields[22]));
		if (!fields[23].isEmpty())
			record.setTimestamp(Long.parseLong(fields[23]));
		if (!fields[24].isEmpty())
			record.setDnt(Integer.parseInt(fields[24]));
		if (!fields[25].isEmpty())
			record.setSession_depth(Integer.parseInt(fields[25]));
		if (!fields[26].isEmpty())
			record.setWidth(Integer.parseInt(fields[26]));
		if (!fields[27].isEmpty())
			record.setHeight(Integer.parseInt(fields[27]));
		if (!fields[28].isEmpty())
			record.setMin_width(Integer.parseInt(fields[28]));
		if (!fields[29].isEmpty())
			record.setMin_height(Integer.parseInt(fields[29]));
		if (!fields[30].isEmpty())
			record.setMax_width(Integer.parseInt(fields[30]));
		if (!fields[31].isEmpty())
			record.setMax_height(Integer.parseInt(fields[31]));
		if (!fields[32].isEmpty())
			record.setVideo_width(Integer.parseInt(fields[32]));
		if (!fields[33].isEmpty())
			record.setVideo_height(Integer.parseInt(fields[33]));
		return mapper.writeValueAsString(record);
	}

	private static String getDeviceId(String deviceId) {
		if (isDeviceId(deviceId)) {
			return DigestUtils.sha1Hex(deviceId);
		} else if (isValidSHA(deviceId)) {
			return deviceId.toLowerCase();
		}
		return "";
	}

	private static boolean isDeviceId(String id) {
		String pattern = "(.*)(-)(.*)(-)(.*)(-)(.*)(-)(.*)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(id);
		return m.matches();

	}

	private static boolean isValidSHA(String id) {
		return (id.matches("[a-fA-F0-9]{40}"));
	}
}