

package OpenRate.record;

import java.util.ArrayList;

/**
 * Flat record is the basic type of record used by the file adapters, and
 * returns the data as a single string, which can then be split and processed
 * as required.
 */
public class FlatRecord extends AbstractRecord
{
  private static final long serialVersionUID = -1506405981820429432L;

  // the original data we received
  private String originalData;

  /**
   * Creates a new instance of FlatRecord
   *
   * @param data The data to map
   * @param RecordNumber The record number
   */
  public FlatRecord(String data, int RecordNumber)
  {
    super();

    this.originalData   = data;
    this.recordNumber   = RecordNumber;
  }

 /**
  * Creates a new instance of FlatRecord
  *
  * @param data The data to map
  */
  public FlatRecord(String data)
  {
    super();
    this.originalData   = data;
  }

  /** Overloaded contructor for derived classes */
  public FlatRecord()
  {
    super();
  }

  /**
   * Get the original data
   *
   * @return The original data
   */
  public String getData()
  {
    return this.originalData;
  }

  /**
   * Set the original data
   *
   * @param DataToSet The data to store
   */
  public void setData(String DataToSet)
  {
    this.originalData = DataToSet;
  }

 /**
  * This returns the dump information. Should be overwritten by the final
  * implementation class
  */
  @Override
  public ArrayList<String> getDumpInfo()
  {
    RecordError tmpError;
    int i;
    int tmpErrorCount;
    ArrayList<String> tmpDumpList;

    tmpDumpList = new ArrayList<>();

    // Get the error count
    tmpErrorCount = this.getErrors().size();

    // Format the fields
    tmpDumpList.add("============== FLAT RECORD ============");
    tmpDumpList.add("  original record = <" + this.originalData + ">");

    tmpDumpList.add("  Errors          = <" + this.getErrors().size() + ">");
    if (tmpErrorCount>0)
    {
      tmpDumpList.add("-------------- ERRORS ----------------");
      for (i = 0 ; i < this.getErrors().size() ; i++)
      {
        tmpError = (RecordError) this.getErrors().get(i);
        tmpDumpList.add("    Error           = <" + tmpError.getMessage() + ">");
      }
    }

    return tmpDumpList;
  }
}
