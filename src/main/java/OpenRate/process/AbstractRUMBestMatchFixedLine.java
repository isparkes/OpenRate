

package OpenRate.process;

import OpenRate.record.ChargePacket;
import OpenRate.record.ErrorType;
import OpenRate.record.RatingRecord;
import OpenRate.record.RecordError;
import java.util.ArrayList;

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
public abstract class AbstractRUMBestMatchFixedLine extends AbstractBestMatchFixedLine
{
 /**
  * This performs the best match for each of the charge packets
  *
  * @param RecordToMatch The record we are matching on
  * @param A_Number The A Number to match
  * @param B_Number The B Number to match
  */
  public void performRUMBestMatch(RatingRecord RecordToMatch, String A_Number, String B_Number)
  {
    int          Index;
    RecordError  tmpError;
    ChargePacket tmpCP;
    String       BestMatchZone;
    boolean      Errored = false;

    if (RecordToMatch instanceof RatingRecord)
    {
      RatingRecord CurrentRecord = (RatingRecord) RecordToMatch;

      try
      {
        // Cycle over the charge packets gathering the impacts as we go
        for (Index = 0 ; Index < CurrentRecord.getChargePacketCount() ; Index++)
        {
          tmpCP = CurrentRecord.getChargePacket(Index);

          if (tmpCP.Valid)
          {
            // Use the time zoning from AbstractTimeMatch on this charge packet
            BestMatchZone = BM.getBestMatchFixedLine(tmpCP.zoneModel, A_Number, B_Number);

            if (isValidBestMatchResult(BestMatchZone) == false)
            {
              // We only want one error on the record
              if (!Errored)
              {
                tmpError = new RecordError("ERR_ZONE_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                CurrentRecord.addError(tmpError);
                Errored = true;
              }
            }

            // Write the information back into the record
            tmpCP.zoneResult = BestMatchZone;
          }
        }
      }
      catch (Exception e)
      {
        tmpError = new RecordError("ERR_ZONE_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
        CurrentRecord.addError(tmpError);
      }
    }
    else
    {
      tmpError = new RecordError("ERR_NOT_RATING_RECORD", ErrorType.SPECIAL, getSymbolicName());
      RecordToMatch.addError(tmpError);
    }
  }

 /**
  * This performs the best match for each of the charge pakets
  *
  * @param RecordToMatch The record we are matching on
  * @param A_Number The A Number to match
  * @param B_Number The B Number to match
  */
  public void performRUMBestMatchWithChildData(RatingRecord RecordToMatch, String A_Number, String B_Number)
  {
    int               Index;
    RecordError       tmpError;
    ChargePacket      tmpCP;
    ArrayList<String> BestMatchZone;
    boolean           Errored = false;

    if (RecordToMatch instanceof RatingRecord)
    {
      RatingRecord CurrentRecord = (RatingRecord) RecordToMatch;

      // Cycle over the charge packets gathering the impacts as we go
      for (Index = 0 ; Index < CurrentRecord.getChargePacketCount() ; Index++)
      {
        tmpCP = CurrentRecord.getChargePacket(Index);

        if (tmpCP.Valid)
        {
          try
          {
            // Use the time zoning from AbstractTimeMatch on this charge packet
            BestMatchZone = BM.getBestMatchFixedLineWithChildData(tmpCP.zoneModel, A_Number, B_Number);

            if (isValidBestMatchResult(BestMatchZone) == false)
            {
              // We only want one error on the record
              if ((!Errored) & (tmpCP.priority == 0))
              {
                tmpError = new RecordError("ERR_ZONE_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                CurrentRecord.addError(tmpError);
                Errored = true;
              }
            }
            else
            {
              // we have something valid to write
              fillCPWithBestMatchChildData(RecordToMatch, tmpCP, BestMatchZone);
            }
          }
          catch (Exception e)
          {
            tmpError = new RecordError("ERR_ZONE_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
            CurrentRecord.addError(tmpError);
          }
        }
      }
    }
    else
    {
      tmpError = new RecordError("ERR_NOT_RATING_RECORD", ErrorType.SPECIAL, getSymbolicName());
      RecordToMatch.addError(tmpError);
    }
  }

 /**
  * This method is intended to be overwritten in the case that the standard
  * best match processing is to be changed. By default, it fills the zoneResult
  * field with the first parameter that is recovered and the zoneInfo with the
  * second.
  *
  * @param Record The record to perform the match on
  * @param tmpCP The Charge Packet we are working on
  * @param BestMatchZone The Best match data ArrayList
  */
  protected void fillCPWithBestMatchChildData(RatingRecord Record, ChargePacket tmpCP, ArrayList<String> BestMatchZone)
  {
    if (BestMatchZone.size() > 0)
    {
      tmpCP.zoneResult = BestMatchZone.get(0);
    }

    if (BestMatchZone.size() > 1)
    {
      tmpCP.zoneInfo = BestMatchZone.get(1);
    }
  }
}