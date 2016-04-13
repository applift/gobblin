package gobblin.applift.parquet;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;

public class AvroReqLogPolicy extends RowLevelPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(AvroReqLogPolicy.class);

  public AvroReqLogPolicy(State state, Type type) {
    super(state, type);
  }

  @Override
  public Result executePolicy(Object record) {
    GenericRecord logRecord = (GenericRecord) record;
    try {
      GenericRecord reqInfoRecord = (GenericRecord) logRecord.get("req_info");
      if (reqInfoRecord == null || reqInfoRecord.get("unix_ts") == null)
        return Result.FAILED;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.warn("Applift: Faulty record : " + logRecord.toString());
      return Result.FAILED;
    }
    return Result.PASSED;
  }
}
