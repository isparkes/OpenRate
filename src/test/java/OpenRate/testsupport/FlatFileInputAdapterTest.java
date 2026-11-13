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
package OpenRate.testsupport;

import OpenRate.adapter.file.FlatFileInputAdapter;
import OpenRate.record.FlatRecord;
import OpenRate.record.HeaderRecord;
import OpenRate.record.IRecord;
import OpenRate.record.TrailerRecord;

/**
 * Very basic flat file input adapter for test purposes. Reads FlatRecords in
 * and passes them on nearly unmodified (only record number is set) for 
 * processing.
 *
 * @author ian
 */
public class FlatFileInputAdapterTest
        extends FlatFileInputAdapter {

  //  This is the stream record number counter which tells us the number of the compressed records
  private int streamRecordNumber;

  @Override
  public HeaderRecord procHeader(HeaderRecord r) {
    // get the stream name and log it
    String tmpStreamName = r.getStreamName();
    getPipeLog().info("Opening file " + tmpStreamName);

    // reset the record numbering
    streamRecordNumber = 0;

    return r;
  }

  @Override
  public IRecord procValidRecord(FlatRecord r) {
    getPipeLog().info("Got valid record with data: " + r.getData());
    
    // Number the record
    r.recordNumber = streamRecordNumber;
    streamRecordNumber++;

    return (IRecord) r;
  }

  @Override
  public IRecord procErrorRecord(FlatRecord r) {
    // The FlatFileInputAdapter is not able to create error records, so we
    // do not have to do anything for this
    return r;
  }

  @Override
  public TrailerRecord procTrailer(TrailerRecord r) {
    r.setRecordCount(streamRecordNumber);

    // get the stream name and log it
    String tmpStreamName = r.getStreamName();
    getPipeLog().info("Opening file " + tmpStreamName);
    
    return r;
  }
}
