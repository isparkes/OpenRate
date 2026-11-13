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

package ExampleApplications.SimpleApplication;

import OpenRate.adapter.file.FlatFileOutputAdapter;
import OpenRate.record.FlatRecord;
import OpenRate.record.IRecord;
import java.util.ArrayList;
import java.util.Collection;

/**
 * The Output Adapter is responsible for writing the completed records to the
 * target file.
 */
public class SimpleSTOutputAdapter
  extends FlatFileOutputAdapter
{

 /**
  * Constructor for SimpleOutputAdapter.
  */
  public SimpleSTOutputAdapter()
  {
    super();
  }

 /**
  * We transform the records here so that they are ready to output making any
  * specific changes to the record that are necessary to make it ready for
  * output.
  *
  * As we are using the FlatFileOutput adapter, we should transform the records
  * into FlatRecords, storing the data to be written using the setData() method.
  * This means that we do not have to know about the internal workings of the
  * output adapter.
  *
  * Note that this is just undoing the transformation that we did in the input
  * adapter.
   * @return 
  */
  @Override
  public Collection<FlatRecord> procValidRecord(IRecord r)
  {
    FlatRecord tmpOutRecord;
    SimpleRecord tmpInRecord;

    Collection<FlatRecord> Outbatch;
    Outbatch = new ArrayList<>();

    tmpOutRecord = new FlatRecord();
    tmpInRecord = (SimpleRecord)r;
    tmpOutRecord.setData(tmpInRecord.unmapOriginalData());

    Outbatch.add(tmpOutRecord);

    return Outbatch;
  }

 /**
  * Handle any error records here so that they are ready to output making any
  * specific changes to the record that are necessary to make it ready for
  * output.
   * @return 
  */
  @Override
  public Collection<FlatRecord> procErrorRecord(IRecord r)
  {
    return null;
  }
}
