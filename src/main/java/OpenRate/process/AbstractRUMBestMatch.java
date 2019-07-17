

package OpenRate.process;

import OpenRate.exception.ProcessingException;
import OpenRate.record.ChargePacket;
import OpenRate.record.ErrorType;
import OpenRate.record.RatingRecord;
import OpenRate.record.RecordError;
import java.util.ArrayList;

/**
 * This class implements a zoning module, based on the RUM rating model.
 * That means that Charge Packets are evaluated during the processing and the
 * results are generally written back to the Charge Packet.
 *
 * This module performs splitting in the following cases:
 *  - The Splitting Flag in the Charge Packet has to be set to a value which
 *    indicates that splitting is required:
 *    - 0 = no splitting
 *    - 1 = splitting equally of the duration over the packets
 *
 * The duration of the call is taken from the RUMValue field, and this is
 * interpreted as the duration of the call. In cases where you do not want
 * the RUMValue interpreted as the duration, do NOT set the splitting flag. No
 * filtering is done on the RUM type, only the splitting flag is significant.
 *
 * This module can create many charge packets out of each 'seed' charge packet,
 * one for each zone that has been impacted. Zones of a similar type (e.g.
 * 'peak' or 'off-peak' are not aggregted, but instead are presented as
 * individual segements of validity, and should be rolled-up if necessary after
 * rating.
 *
 * Fields Read:
 *  - Charge Packet:zoneModel
 *  - Charge Packet:Valid
 *  - Charge Packet:priority
 *  - Parameter:Destination
 *
 * Fields Written:
 *  - Charge Packet:zoneResult
 *
 */
public abstract class AbstractRUMBestMatch extends AbstractBestMatch
{
  // this tells us whether to deal with the exception ourself, pr pass it to the
  // parent module for handling
  private boolean reportExceptions = true;

  // -----------------------------------------------------------------------------
  // ------------------------ Start of custom functions --------------------------
  // -----------------------------------------------------------------------------

 /**
  * Set the state of the exception reporting. True means that we let the parent
  * module deal with it, false means that we deal with it ourselves.
  *
  * @param NewValue
  */
  public void setExceptionReporting(boolean NewValue)
  {
    reportExceptions = NewValue;
  }

 /**
  * This performs the best match for each of the charge pakets. Each record has
  * one destination number, but may require matching against multiple zone
  * models, thus the match is done by matching the destination number against
  * the zone model recovered from each of the charge packets.
  *
  * @param RecordToMatch The record to work on
  * @param Destination The destination number to match on
  * @throws ProcessingException
  */
  public void performRUMBestMatch(RatingRecord RecordToMatch, String Destination)
    throws ProcessingException
  {
    int          Index;
    RecordError  tmpError;
    ChargePacket tmpCP;
    String       BestMatchZone;
    boolean      Errored = false;

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
            BestMatchZone = this.getBestMatch(tmpCP.zoneModel, Destination);

            if (isValidBestMatchResult(BestMatchZone) == false)
            {
              // We only want one error on the record
              if ((!Errored) & (tmpCP.priority == 0))
              {
                  if (reportExceptions == false)
                  {
                    // Only error if this is a base packet
                    tmpError = new RecordError("ERR_ZONE_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                    CurrentRecord.addError(tmpError);
                    Errored = true;
                  }
                  else
                  {
                    throw new ProcessingException("Zone match not found",getSymbolicName());
                  }
              }
            }

            // Write the information back into the record
            tmpCP.zoneResult = BestMatchZone;
          }
          catch (Exception e)
          {
            // Only error if this is a base packet
            if (tmpCP.priority == 0)
            {
              if (reportExceptions == false)
              {
                // we deal with it
                tmpError = new RecordError("ERR_ZONE_MATCH_ERROR", ErrorType.SPECIAL, getSymbolicName(), e.getMessage());
                tmpError.setModuleName(getSymbolicName());
                CurrentRecord.addError(tmpError);
              }
              else
              {
                throw new ProcessingException(e,getSymbolicName());
              }
            }
          }
        }
      }
    }
    else
    {
      if (reportExceptions == false)
      {
        tmpError = new RecordError("ERR_NOT_RATING_RECORD", ErrorType.SPECIAL, getSymbolicName());
        RecordToMatch.addError(tmpError);
      }
      else
      {
        throw new ProcessingException("Not a rating record",getSymbolicName());
      }
    }
  }

 /**
  * This performs the best match for each of the charge pakets. Each record has
  * one destination number, but may require matching against multiple zone
  * models, thus the match is done by matching the destination number against
  * the zone model recovered from each of the charge packets.
  *
  * For each charge packet that matches, the fillCP method will be called.
  *
  * @param RecordToMatch The record to work on
  * @param Destination The destination number to match on
  * @throws ProcessingException
  */
  public void performRUMBestMatchWithChildData(RatingRecord RecordToMatch, String Destination)
    throws ProcessingException
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
            BestMatchZone = this.getBestMatchWithChildData(tmpCP.zoneModel, Destination);

            if (BestMatchZone == null)
            {
              if (reportExceptions == false)
              {
                // Only error if this is a base packet
                tmpError = new RecordError("ERR_ZONE_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                CurrentRecord.addError(tmpError);
                Errored = true;
              }
              else
              {
                throw new ProcessingException("Zone match not found",getSymbolicName());
              }
            }
            else
            {
              if (isValidBestMatchResult(BestMatchZone) == false)
              {
                // We only want one error on the record
                if ((!Errored) & (tmpCP.priority == 0))
                {
                  if (reportExceptions == false)
                  {
                    // Only error if this is a base packet
                    tmpError = new RecordError("ERR_ZONE_NOT_FOUND", ErrorType.SPECIAL, getSymbolicName());
                    CurrentRecord.addError(tmpError);
                    Errored = true;
                  }
                  else
                  {
                    throw new ProcessingException("Zone match not found",getSymbolicName());
                  }
                }
              }
              else
              {
                // we have something valid to write
                fillCPWithBestMatchChildData(RecordToMatch, tmpCP, BestMatchZone);
              }
            }
          }
          catch (Exception e)
          {
            // Only error if this is a base packet
            if (tmpCP.priority == 0)
            {
              if (reportExceptions == false)
              {
                // we deal with it
                tmpError = new RecordError("ERR_ZONE_MATCH_ERROR", ErrorType.SPECIAL,e.getMessage());
                tmpError.setModuleName(getSymbolicName());
                CurrentRecord.addError(tmpError);
              }
              else
              {
                throw new ProcessingException(e,getSymbolicName());
              }
            }
          }
        }
      }
    }
    else
    {
      if (reportExceptions == false)
      {
        tmpError = new RecordError("ERR_NOT_RATING_RECORD", ErrorType.SPECIAL, getSymbolicName());
        RecordToMatch.addError(tmpError);
      }
      else
      {
        throw new ProcessingException("Not a rating record",getSymbolicName());
      }
    }
  }

 /**
  * This method is intended to be overwritten in the case that the standard
  * best match processing is to be changed. By default, it fills the zoneResult
  * field with the first parameter that is recovered and the zoneInfo with the
  * second.
  *
  * @param Record The record we are to fill
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