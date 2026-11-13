/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package OpenRate.record;

import java.util.ArrayList;

/**
 * Flat record is the basic type of record used by the file adapters, and
 * returns the data as a single string, which can then be split and processed
 * as required.
 */
public class HeaderRecord extends AbstractRecord
{
  private static final long serialVersionUID = 2340874768931864174L;

  // this holds the base name of the file we are working on
  private String StreamName = null;

  // This is used to pass the transaction number down the pipe
  private int TransactionNumber = 0;

  /** Overloaded constructor for derived classes */
  public HeaderRecord()
  {
    super();

    // mark the record as invalid so that it does not take part in the
    // processing
    this.setValid(false);
  }

 /**
  * This returns the dump information.
  */
  @Override
  public ArrayList<String> getDumpInfo()
  {
    ArrayList<String> tmpDumpList;
    tmpDumpList = new ArrayList<>();

    // Format the fields
    tmpDumpList.add("============ STREAM HEADER ===========");
    tmpDumpList.add("  Stream name    = <" + this.StreamName + ">");
    tmpDumpList.add("  Transaction    = <" + this.TransactionNumber + ">");

    return tmpDumpList;
  }

 /**
  * Set the stream level base name, so that non transactional modules can also
  * access the inforamtion if they need.
  *
  * @param NewStreamName The stream name to set
  */
  public void setStreamName(String NewStreamName)
  {
    StreamName = NewStreamName;
  }

 /**
  * Return the stream base name for anyone that needs it
  *
  * @return The stream name of the stream
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
  * @return The transaction number of this stream
  */
  public int getTransactionNumber()
  {
    return TransactionNumber;
  }
}
