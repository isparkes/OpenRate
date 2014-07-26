/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2014.
 *
 * The following restrictions apply unless they are expressly relaxed in a
 * contractual agreement between the license holder or one of its officially
 * assigned agents and you or your organisation:
 *
 * 1) This work may not be disclosed, either in full or in part, in any form
 *    electronic or physical, to any third party. This includes both in the
 *    form of source code and compiled modules.
 * 2) This work contains trade secrets in the form of architecture, algorithms
 *    methods and technologies. These trade secrets may not be disclosed to
 *    third parties in any form, either directly or in summary or paraphrased
 *    form, nor may these trade secrets be used to construct products of a
 *    similar or competing nature either by you or third parties.
 * 3) This work may not be included in full or in part in any application.
 * 4) You may not remove or alter any proprietary legends or notices contained
 *    in or on this work.
 * 5) This software may not be reverse-engineered or otherwise decompiled, if
 *    you received this work in a compiled form.
 * 6) This work is licensed, not sold. Possession of this software does not
 *    imply or grant any right to you.
 * 7) You agree to disclose any changes to this work to the copyright holder
 *    and that the copyright holder may include any such changes at its own
 *    discretion into the work
 * 8) You agree not to derive other works from the trade secrets in this work,
 *    and that any such derivation may make you liable to pay damages to the
 *    copyright holder
 * 9) You agree to use this software exclusively for evaluation purposes, and
 *    that you shall not use this software to derive commercial profit or
 *    support your business or personal activities.
 *
 * This software is provided "as is" and any expressed or impled warranties,
 * including, but not limited to, the impled warranties of merchantability
 * and fitness for a particular purpose are disclaimed. In no event shall
 * The OpenRate Project or its officially assigned agents be liable to any
 * direct, indirect, incidental, special, exemplary, or consequential damages
 * (including but not limited to, procurement of substitute goods or services;
 * Loss of use, data, or profits; or any business interruption) however caused
 * and on theory of liability, whether in contract, strict liability, or tort
 * (including negligence or otherwise) arising in any way out of the use of
 * this software, even if advised of the possibility of such damage.
 * This software contains portions by The Apache Software Foundation, Robert
 * Half International.
 * ====================================================================
 */

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