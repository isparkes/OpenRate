
package ExampleApplications.SimpleApplication;

import OpenRate.process.AbstractBestMatch;
import OpenRate.record.ErrorType;
import OpenRate.record.IRecord;
import OpenRate.record.RecordError;

/**
 * This class is an example of a plug in that does only a lookup, and thus
 * does not need to be registered as transaction bound. Recall that we will
 * only need to be transaction aware when we need some specific information
 * from the transaction management (e.g. the base file name) or when we
 * require to have the possibility to undo transaction work in the case of
 * some failure.
 *
 * In this case we do not need it, as the input and output adapters will roll
 * the information back for us (by removing the output stream) in the case of
 * an error.
 */
public class DestLookup extends AbstractBestMatch
{
  /**
  * This is called when a data record is encountered. You should do any normal
  * processing here.
  */
  @Override
  public IRecord procValidRecord(IRecord r)
  {

    RecordError tmpError;
    String ZoneValue;
    SimpleRecord CurrentRecord = (SimpleRecord)r;

    try
    {
      // We only transform the basic records, and leave the others alone
      if (CurrentRecord.RECORD_TYPE == 20)
      {
        // Look up the Destination
        ZoneValue = getBestMatch("DEF", CurrentRecord.B_Number);

        // Write the information back into the record
        CurrentRecord.Destination = ZoneValue;
      }
    }
    catch (Exception e)
    {
      // error detected,
      tmpError = new RecordError("ERR_ZONE_MATCH_FAILED", ErrorType.SPECIAL);
      CurrentRecord.addError(tmpError);
    }

    return r;
  }

  /**
  * This is called when a data record with errors is encountered. You should do
  * any processing here that you have to do for error records, e.g. statistics,
  * special handling, even error correction!
  */
  @Override
  public IRecord procErrorRecord(IRecord r)
  {
    return r;
  }
}
