package gobblin.applift.parquet;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.metrics.kafka.SchemaRegistryException;

/**
 * Thread safe.
 * 
 * 
 */
public class KafkaAvroSchemaRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSchemaRegistry.class);
  public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
  private final Properties props;
  private final String url;
  private final HttpClient httpClient;

  protected KafkaAvroSchemaRegistry(Properties props) {
    this.props = props;
    this.url = this.props.getProperty(KAFKA_SCHEMA_REGISTRY_URL);
    this.httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
  }

  public synchronized void register(Schema schema, String topicName) throws SchemaRegistryException {
    LOG.info("Registering schema " + schema.toString());
    String registerUrl = url + "/" + topicName + "/register";
    
    PutMethod put = new PutMethod(registerUrl);
    put.setRequestBody(schema.toString());

    try {
      LOG.debug("Loading: " + put.getURI());
      int statusCode = httpClient.executeMethod(put);
      if (statusCode != HttpStatus.SC_OK) {
        throw new SchemaRegistryException("Error occurred while trying to register schema: " + statusCode);
      }

      String response;
      response = put.getResponseBodyAsString();
      if (response != null) {
        LOG.info("Received response " + response);
      }
    } catch (Throwable t) {
      throw new SchemaRegistryException(t);
    } finally {
      put.releaseConnection();
    }
  }

  public synchronized Schema getLatestSchemaByTopic(String topicName) throws SchemaRegistryException {
    String schemaUrl = KafkaAvroSchemaRegistry.this.url + "/" + topicName + "/latest";
    LOG.debug("Fetching from URL : " + schemaUrl);
    GetMethod get = new GetMethod(schemaUrl);

    int statusCode;
    String responseBody;
    try {
      statusCode = KafkaAvroSchemaRegistry.this.httpClient.executeMethod(get);
      responseBody = get.getResponseBodyAsString();
    } catch (HttpException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      get.releaseConnection();
    }

    if (statusCode != HttpStatus.SC_OK) {
      throw new SchemaRegistryException(
          String.format("Latest schema for topic %s cannot be retrieved. Status code = %d", topicName, statusCode));
    }

    Schema schema;
    try {
      String schemaString = responseBody.substring(responseBody.indexOf('{'));
      schema = new Schema.Parser().parse(schemaString);
    } catch (Throwable t) {
      throw new SchemaRegistryException(String.format("Latest schema for topic %s cannot be retrieved", topicName), t);
    }
    return schema;
  }
}
