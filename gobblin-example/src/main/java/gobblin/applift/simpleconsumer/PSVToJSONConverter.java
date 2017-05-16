package gobblin.applift.simpleconsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;

public class PSVToJSONConverter extends Converter<Object, Object, String, String>{
  ObjectMapper mapper = new ObjectMapper();
  
  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterable<String> convertRecord(Object outputSchema, String inputRecord,
      WorkUnitState workUnit) throws DataConversionException { 
    String[] psvRecords = inputRecord.split("\n");
    List<String> jsonRecords = new ArrayList<String>();
    for (String psvRecord : psvRecords) {
      try {
        jsonRecords.add(psvToJson(psvRecord));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return jsonRecords;
  }

  private String psvToJson(String psvRecord) throws IOException {
    String[] fields = psvRecord.split("\\|");
    ClickHouseRecord record = new ClickHouseRecord();
    record.setExchangeId(fields[0]);
    record.setPublisherId(fields[1]);
    record.setBundleId(fields[3]);
    record.setAppId(fields[4]);
    record.setCountryId(fields[6]);
    record.setOs(fields[10]);
    record.setAge(Integer.parseInt(fields[15]));
    record.setGender(fields[16]);
    record.setDeviceId(getDeviceId(fields[22]));
    record.setTimestamp(Long.parseLong(fields[23]));
    record.setRequest(1);
    record.setImp(0);
    record.setClick(0);
    record.setConversion(0);
    record.setBid(0); 
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