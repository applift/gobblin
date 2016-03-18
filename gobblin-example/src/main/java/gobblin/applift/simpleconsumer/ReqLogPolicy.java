package gobblin.applift.simpleconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;

public class ReqLogPolicy extends RowLevelPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(ReqLogPolicy.class);
  
  public ReqLogPolicy(State state, Type type) {
    super(state, type);
  }

  @Override
  public Result executePolicy(Object record) {
    String logRecord = (String) record;
    boolean isLogRotate = logRecord.contains("LOGROTATE");
    if (isLogRotate) {
      return Result.FAILED;
    }
    JsonElement element;
    try {
      element = new JsonParser().parse(logRecord);
    } catch (JsonSyntaxException e) {
      return Result.FAILED;
    }
    try {
      JsonObject reqLogObject = element.getAsJsonObject();
      JsonObject reqInfoObject = reqLogObject.getAsJsonObject("req_info");
      if (reqInfoObject == null || reqInfoObject.get("unix_ts") == null)
        return Result.FAILED;
    } catch (Exception e) {
      LOG.warn("Applift: Faulty record : "+logRecord);
      return Result.FAILED;
    }
    return Result.PASSED;
  }
}
