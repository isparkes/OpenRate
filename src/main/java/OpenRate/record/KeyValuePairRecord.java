
package OpenRate.record;

import java.util.ArrayList;
import java.util.Map;

/**
 * Flat record is the basic type of record used by the file adapters, and
 * returns the data as a single string, which can then be split and processed as
 * required.
 */
public class KeyValuePairRecord extends AbstractRecord {

  private static final long serialVersionUID = -1506405981820429430L;

  // the original data we received
  private Map<String, String> originalData;

  /**
   * Creates a new instance of FlatRecord
   *
   * @param data The data to map
   * @param RecordNumber The record number
   */
  public KeyValuePairRecord(Map<String, String> data, int RecordNumber) {
    super();

    this.originalData = data;
    this.recordNumber = RecordNumber;
  }

  /**
   * Creates a new instance of FlatRecord
   *
   * @param data The data to map
   */
  public KeyValuePairRecord(Map<String, String> data) {
    super();

    this.originalData = data;
  }

  /**
   * Overloaded constructor for derived classes
   */
  public KeyValuePairRecord() {
    super();
  }

  /**
   * Get the original data
   *
   * @return The original data
   */
  public Map<String, String> getData() {
    return this.originalData;
  }

  /**
   * Set the original data
   *
   * @param dataToSet The data to store
   */
  public void setData(Map<String, String> dataToSet) {
    this.originalData = dataToSet;
  }

  /**
   * This returns the dump information. Should be overwritten by the final
   * implementation class
   *
   * @return
   */
  @Override
  public ArrayList<String> getDumpInfo() {
    RecordError tmpError;
    int i;
    int tmpErrorCount;
    ArrayList<String> tmpDumpList;

    tmpDumpList = new ArrayList<>();

    // Get the error count
    tmpErrorCount = this.getErrors().size();

    // Format the fields
    tmpDumpList.add("============== KVP RECORD =============");
    for (String key : originalData.keySet()) {
      tmpDumpList.add("  KVP             = <" + key + ":" + originalData.get(key) + ">");
    }

    tmpDumpList.add("  Errors          = <" + this.getErrors().size() + ">");
    if (tmpErrorCount > 0) {
      tmpDumpList.add("-------------- ERRORS ----------------");
      for (i = 0; i < this.getErrors().size(); i++) {
        tmpError = (RecordError) this.getErrors().get(i);
        tmpDumpList.add("    Error           = <" + tmpError.getMessage() + ">");
      }
    }

    return tmpDumpList;
  }
}
