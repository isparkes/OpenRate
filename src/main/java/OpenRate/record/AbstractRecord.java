/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2015.
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

package OpenRate.record;

import java.util.ArrayList;
import java.util.List;

/**
 * IRecord implementation fleshing out the most basic elements of the IRecord
 * interface. The basic functions handled by this record implementation are:
 *  - Standard record error handling
 *  - Standard record output handling
 *  - record validity
 *  - record dump flagging
 *  - record type
 *  - record id
 */
public abstract class AbstractRecord implements IRecord
{
  /**
   * Default UID for Serializable class
   */
  private static final long serialVersionUID = -1971022798790808812L;
  // Errors that this record has
  private List<IError> errors = new ArrayList<>();

  /**
   * The record number is the sequential number of the record in the stream.
   */
  public int RecordNumber;

 /**
  * the record type - integer for speed
  * The record type is what allows us to determine what the records to handle
  * are, and what to ignore. This is the internal version of the information
  * which basically tells us if this record is a detail CDR or a header or
  * trailer
  */
  public int RECORD_TYPE = 0;

  /**
   * This tells us whether to bother processing the record or not
   * Usually this will only be true for records that really need processing
   * (headers and trailers can be ignored, for example)
   */
  public boolean validRecord = true;

  /**
   *This tells us if the record is a real time record or not
   */
  public boolean RTRecord = false;

  /**
   * Outputs that this record should go to
   */
  public ArrayList<String> outputs = new ArrayList<>();

  // These are for the tracking of the outputting via the output adapter chain
  // Each time we add an output, we increment the OutputsAssigned, each time
  // an output adapter writes, OutputsWritten is incremented, when we have
  // written all of the outputs, the record can be destroyed. If OutputsAssigned
  // is 0, all outputs are written
  private int outputsWritten = 0;

  /**
   * Used for the dump flagging. If this is set to true and the dump is set to
   * dump flagged records, then the record will be dumped.
   */
  public boolean dumpRecord = false;


  /**
   *
   */
  public Object currentStateObject = null;

 /**
  * default constructor
  */
  public AbstractRecord()
  {
    // Nop
  }

 /**
  * Is the Record valid. This is a flag that, when true, means that processing
  * modules should do their thing with the record. The reasons for this being
  * false are that it is not a real record (e.g. it is a control record) or
  * that it is a record that has an error attached to it that means it is
  * not worth the effort of processing.
  *
  * @return True if the record is marked as valid, otherwise false
  */
  @Override
  public boolean isValid()
  {
    return validRecord;
  }

 /**
  * Set the state of the valid flag to the new defined value
  *
  * @param NewValue The new state of the valid flag
  */
  @Override
  public void setValid(boolean NewValue)
  {
    validRecord = NewValue;
  }

 /**
  * Is the Record going to be dumped - this flag allows you to decide if a
  * record is to be dumped.
  *
  * @return True if the record is marked as a record to dump, otherwise false
  */
  @Override
  public boolean isDump()
  {
    return dumpRecord;
  }

 /**
  * Set the state of the dump flag to the new defined value
  *
  * @param NewValue The new state of the dump flag
  */
  @Override
  public void setDump(boolean NewValue)
  {
    dumpRecord = NewValue;
  }

 /**
  * Is the Record a real time record. This is a flag that, when true, means that
  * the record has arrived via a real time (i.e. priority) feed
  *
  * @return True if the record is real time, otherwise false
  */
  @Override
  public boolean isRealtime()
  {
    return RTRecord;
  }

 /**
  * Set the state of the real time flag to the new defined value
  *
  * @param NewValue The new state of the real time flag
  */
  @Override
  public void setRealtime(boolean NewValue)
  {
    RTRecord = NewValue;
  }

  /**
   * Get if the record has an error attached to it.
   *
   * If isErrored() returns true, the getErrors() method should
   * return a List with size() > 1. If the error list is empty,
   * isErrored should be false.
   *
   * @return True if the record is marked as errored, otherwise false
   */
  @Override
  public boolean isErrored()
  {
    return errors.size() > 0;
  }

  /**
   * Return the list of errors attached to this record.
   * Allows a cumulative process for attaching errors
   * to the record where the first error in the list is
   * the first error found during processing.
   *
   * @return List of the errors associated with the record
   */
  @Override
  public List<IError> getErrors()
  {
    return this.errors;
  }

  /**
   * Return the number of errors attached to this record.
   *
   * @return List of the errors associated with the record
   */
  @Override
  public int getErrorCount()
  {
    return this.errors.size();
  }

  /**
   * Add a single error to the error list for this record
   *
   * @param error The new error to add
   */
  @Override
  public void addError(IError error)
  {
    if (error.getModuleName().isEmpty())
    {
      // get the name of the calling class
      error.setModuleName(new Throwable().fillInStackTrace().getStackTrace()[1].getClassName());
    }

    // Add the error to the list
    errors.add(error);

    // Mark by default that the errored record is not valid
    this.setValid(false);
  }

  /**
   * Clear the errors for this record
   */
  @Override
  public void clearErrors()
  {
    errors.clear();

    // Reset the record valid
    this.setValid(true);
  }

  /**
   * Set a single error to the error list for this record
   *
   * @param Index The index of the error to set
   * @param error The new error to add
   */
  public void setError(int Index, IError error)
  {
    errors.set(Index, error);
  }

  /**
   * Set a list of errors on the record. This overwrites any existing errors.
   *
   * @param errors New error list to substitute the old list
   */
  public void setErrors(List<IError> errors)
  {
    this.errors = errors;

    // Mark by default that the errored record is not valid
    this.setValid(false);
  }

  /**
   * Add an output to be written in the output adapter chain
   *
   * @param OutputToAdd The name of the output to add
   */
  public void addOutput(String OutputToAdd)
  {
    outputs.add(OutputToAdd);
  }

  /**
   * Check if we should write to the given output. True if we should write,
   * otherwise false. In the default case of not having defined outputs, all
   * outputs are written to.
   *
   * @param OutputToCheck The name of the output to check
   * @return True if the output was in the output list, otherwise false
   */
  @Override
  public boolean getOutput(String OutputToCheck)
  {
    int i;

    if (outputs.isEmpty())
    {
      // we do not have outputs defined, so by default we write to all outputs
      return true;
    }
    else
    {
      if (outputs.size() == outputsWritten)
      {
        // we have written all the outputs we need to
        return false;
      }

      for ( i = 0 ; i < outputs.size() ; i++)
      {
        if (outputs.get(i).equalsIgnoreCase(OutputToCheck))
        {
          outputsWritten++;
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Return a list of all the outputs that we are to write to.
   *
   * @return Array list of the outputs
   */
  public ArrayList<String> getOutputs()
  {
    return outputs;
  }

  /**
   * Clear the list of outputs that we are to write to.
   */
  public void clearOutputs()
  {
    outputs.clear();
  }

  /**
   * Consume the given output. Return whether the record is completely
   * consumed or not
   *
   * @param OutputToDelete The name of the output to remove
   * @param TerminatingAdapter True if this is the terminating adapter
   * @return True if all outputs were consumed
   */
  @Override
  public boolean deleteOutput(String OutputToDelete, boolean TerminatingAdapter)
  {
    if (outputs.size() > 0)
    {
      if (outputsWritten >= outputs.size())
      {
        // Consumed all outputs
        return true;
      }
      else
      {
        // Not all outputs consumed
        return false;
      }
    }
    else
    {
      // return true for the case that we are not managing outputs and this
      // is the terminating adapter
      return TerminatingAdapter;
    }
  }

 /**
  * This method allows the internal record ID to be set
  */
  @Override
  public void setRecordID(int newRecordID)
  {
    this.RecordNumber = newRecordID;
  }

 /**
  * This method allows the internal record ID to be retrieved
  */
  @Override
  public int getRecordID()
  {
    return this.RecordNumber;
  }

 /**
  * Used to provide diagnostic information for the dumpRecord module
  *
  * @return The dump strings
  */
  @Override
  public abstract ArrayList<String> getDumpInfo();


  /**
   * Return the current state for the ProcessStateLogger
   *
   * @return The current state object
   */
  @Override
  public Object getCurrentStateObject()
  {
    return currentStateObject;
  }

  /**
   * Set the current state for the ProcessStateLogger
   *
   * @param currentStateObject
   */
  @Override
  public void setCurrentStateObject(Object currentStateObject)
  {
    this.currentStateObject = currentStateObject;
  }

 /**
  * Get the Error dump information for the errors associated with this record
  * using default padding
  *
  * @return Error dumpRecord Information
  */
  public ArrayList<String> getErrorDump()
  {
    return getErrorDump(24, false);
  }

 /**
  * Get the Error dump information for the errors associated with this record
  * with a specified padding
  *
  * @param padding The number of characters to pad to
  * @return Error dumpRecord Information
  */
  public ArrayList<String> getErrorDump(int padding)
  {
    return getErrorDump(padding, false);
  }

 /**
  * Get the Error dump information for the errors associated with this record
  * with a specified padding and control of whether module name is shown
  *
  * @param padding The number of characters to pad to
  * @param showFullInfo if true show the module name that raised the error
  * @return Error dumpRecord Information
  */
  public ArrayList<String> getErrorDump(int padding, boolean showFullInfo)
  {
    int i;
    RecordError tmpError;
    ArrayList<String> tmpDumpList = new ArrayList<>();

    // set up the padding
    if (padding < 19) padding = 19;
    String pad = "                                                  ".substring(1, padding - 18);

    int tmpErrorCount = this.getErrors().size();
    tmpDumpList.add("  Errors           " + pad + "= <" + tmpErrorCount + ">");
    if (tmpErrorCount>0)
    {
      tmpDumpList.add("-------------- ERRORS ----------------");
      for (i = 0 ; i < tmpErrorCount ; i++)
      {
        tmpError = (RecordError) this.getErrors().get(i);
        tmpDumpList.add("    Error          " + pad + "= <" + tmpError.getMessage() + ">");

        if (showFullInfo && (tmpError.getModuleName().isEmpty() == false))
        {
          tmpDumpList.add("    Module         " + pad + "= <" + tmpError.getModuleName() + ">");
          tmpDumpList.add("    Desc           " + pad + "= <" + tmpError.getErrorDescription() + ">");
          tmpDumpList.add("    Number         " + pad + "= <" + tmpError.getErrorNumber() + ">");
        }
      }
    }

    return tmpDumpList;
  }
}
