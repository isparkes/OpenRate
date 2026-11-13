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
/* ====================================================================
 * SNOCS Notification Framework
 * ====================================================================
 */

package OpenRate.record;

import OpenRate.record.AbstractRecord;
import OpenRate.record.RecordError;
import java.util.ArrayList;
import javax.jms.Message;

/**
 * Flat record is the basic type of record used by the file adapters, and
 * returns the data as a single string, which can then be split and processed
 * as required.
 */
public class QueueMessageRecord extends AbstractRecord
{
  private static final long serialVersionUID = -1506405981820429632L;

  // the original data we received
  private Message OriginalMessage;

  /**
   * Creates a new instance of QueueMessageRecord
   *
   * @param msg The message to map
   * @param recordNumber The record number
   */
  public QueueMessageRecord(Message msg, int recordNumber)
  {
    super();

    this.OriginalMessage   = msg;
    this.recordNumber   = recordNumber;
  }

 /**
  * Creates a new instance of FlatRecord
  *
  * @param msg The message to map
  */
  public QueueMessageRecord(Message msg)
  {
    super();
    
    this.OriginalMessage   = msg;
  }

  /** Overloaded constructor for derived classes */
  public QueueMessageRecord()
  {
    super();
  }

  /**
   * Get the original data
   *
   * @return The original data
   */
  public Message getData()
  {
    return this.OriginalMessage;
  }

  /**
   * Set the original data
   *
   * @param msg The message data to store
   */
  public void setData(Message msg)
  {
    this.OriginalMessage = msg;
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
    tmpDumpList.add("  original record = <" + this.OriginalMessage.toString() + ">");

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
