

package OpenRate.record;

import java.util.ArrayList;

/**
 * Flat record is the basic type of record used by the file adapters, and
 * returns the data as a single string, which can then be split and processed
 * as required.
 */
public class TrailerRecord extends AbstractRecord
{
  private static final long serialVersionUID = -3471178671328078800L;

  // record check sum at the end of processing
  private int streamRecordCount = 0;

  // this holds the base name of the file we are working on. Useful also in the
  // trailer so that we do not have to store the name
  private String StreamName = null;

  // This is used to pass the transaction number down the pipe
  private int TransactionNumber = 0;

  /** Overloaded contructor for derived classes */
  public TrailerRecord()
  {
    super();

    // mark the record as invalid so that it does not take part in the
    // processing
    this.setValid(false);
  }

 /**
  * This returns the dump information.
  *
  * @return The base dump info
  */
  @Override
  public ArrayList<String> getDumpInfo()
  {
    ArrayList<String> tmpDumpList = new ArrayList<>();

    // Format the fields
    tmpDumpList.add("============ STREAM TRAILER ==========");
    tmpDumpList.add("  Stream records    = <" + this.streamRecordCount + ">");

    return tmpDumpList;
  }

 /**
  * Set the trailer record count so that modules are able to cross check that
  * they have done everything correctly (if they want)
  *
  * @param NewRecordCount The new record count
  */
  public void setRecordCount(int NewRecordCount)
  {
    streamRecordCount = NewRecordCount;
  }

 /**
  * Return the stream record count
  *
  * @return The current record count
  */
  public int getRecordCount()
  {
    return streamRecordCount;
  }

 /**
  * Set the stream level base name, so that non transactional modules can also
  * access the inforamtion if they need.
  *
  * @param NewStreamName The new stream name to set
  */
  public void setStreamName(String NewStreamName)
  {
    StreamName = NewStreamName;
  }

 /**
  * Return the stream base name for anyone that needs it
  *
  * @return The base name
  */
  public String getStreamName()
  {
    return StreamName;
  }

 /**
  * Set the transaction Number which this is the header record for
  *
  * @param newTransNumber The transaction number to set
  */
  public void setTransactionNumber(int newTransNumber)
  {
    TransactionNumber = newTransNumber;
  }

 /**
  * Return the transaction number for anyone that needs it
  *
  * @return The current transaction number
  */
  public int getTransactionNumber()
  {
    return TransactionNumber;
  }
}
