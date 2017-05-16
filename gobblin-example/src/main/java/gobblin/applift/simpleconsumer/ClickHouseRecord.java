package gobblin.applift.simpleconsumer;

public class ClickHouseRecord {
  private String os;
  private String country_id;
  private String exchange_id;
  private String publisher_id;
  private String app_id;
  private String bundle_id;
  private String device_id;
  private long timestamp;
  private int age;
  private String gender;
  private int request;
  private int bid;
  private int imp;
  private int click;
  private int conversion;
  /**
   * @return the device_id
   */
  public String getDeviceId() {
    return device_id;
  }
  /**
   * @param device_id the device_id to set
   */
  public void setDeviceId(String device_id) {
    this.device_id = device_id;
  }
  /**
   * @return the os
   */
  public String getOs() {
    return os;
  }
  /**
   * @param os the os to set
   */
  public void setOs(String os) {
    this.os = os;
  }
  /**
   * @return the country_id
   */
  public String getCountryId() {
    return country_id;
  }
  /**
   * @param country_id the country_id to set
   */
  public void setCountryId(String country_id) {
    this.country_id = country_id;
  }
  /**
   * @return the exchange_id
   */
  public String getExchangeId() {
    return exchange_id;
  }
  /**
   * @param exchange_id the exchange_id to set
   */
  public void setExchangeId(String exchange_id) {
    this.exchange_id = exchange_id;
  }
  /**
   * @return the publisher_id
   */
  public String getPublisherId() {
    return publisher_id;
  }
  /**
   * @param publisher_id the publisher_id to set
   */
  public void setPublisherId(String publisher_id) {
    this.publisher_id = publisher_id;
  }
  /**
   * @return the app_id
   */
  public String getAppId() {
    return app_id;
  }
  /**
   * @param app_id the app_id to set
   */
  public void setAppId(String app_id) {
    this.app_id = app_id;
  }
  /**
   * @return the bundle_id
   */
  public String getBundleId() {
    return bundle_id;
  }
  /**
   * @param bundle_id the bundle_id to set
   */
  public void setBundleId(String bundle_id) {
    this.bundle_id = bundle_id;
  }
  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }
  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  /**
   * @return the age
   */
  public int getAge() {
    return age;
  }
  /**
   * @param age the age to set
   */
  public void setAge(int age) {
    this.age = age;
  }
  /**
   * @return the gender
   */
  public String getGender() {
    return gender;
  }
  /**
   * @param gender the gender to set
   */
  public void setGender(String gender) {
    this.gender = gender;
  }
  /**
   * @return the request
   */
  public int getRequest() {
    return request;
  }
  /**
   * @param request the request to set
   */
  public void setRequest(int request) {
    this.request = request;
  }
  /**
   * @return the bid
   */
  public int getBid() {
    return bid;
  }
  /**
   * @param bid the bid to set
   */
  public void setBid(int bid) {
    this.bid = bid;
  }
  /**
   * @return the imp
   */
  public int getImp() {
    return imp;
  }
  /**
   * @param imp the imp to set
   */
  public void setImp(int imp) {
    this.imp = imp;
  }
  /**
   * @return the click
   */
  public int getClick() {
    return click;
  }
  /**
   * @param click the click to set
   */
  public void setClick(int click) {
    this.click = click;
  }
  /**
   * @return the conversion
   */
  public int getConversion() {
    return conversion;
  }
  /**
   * @param conversion the conversion to set
   */
  public void setConversion(int conversion) {
    this.conversion = conversion;
  }
}
