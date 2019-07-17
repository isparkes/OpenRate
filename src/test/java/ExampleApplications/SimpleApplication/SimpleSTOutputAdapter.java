
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
