/* ====================================================================
 * Limited Evaluation License:
 *
 * The exclusive owner of this work is the OpenRate project.
 * This work, including all associated documents and components
 * is Copyright of the OpenRate project 2006-2013.
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
 * Tiger Shore Management or its officially assigned agents be liable to any
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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * IRecord type used for data being passed through the Pipeline. This is the
 * primitive record ancestor for all records that can pass through a framework
 * so defines the common elements between all record types (even control
 * records), so that we can always have a way of accessing the most fundamental
 * elements of the record.
 */
public interface IRecord extends Serializable
{
  /**
   * CVS version info - Automatically captured and written to the Framework
   * Version Audit log at Framework startup. For more information
   * please <a target='new' href='http://www.open-rate.com/wiki/index.php?title=Framework_Version_Map'>click here</a> to go to wiki page.
   */
  public static String CVS_MODULE_INFO = "OpenRate, $RCSfile: IRecord.java,v $, $Revision: 1.28 $, $Date: 2013-05-13 18:12:11 $";

 /**
  * Is the IRecord a real time record.
  *
  * @return true if the record is a real time record, otherwise false
  */
  public boolean isRealtime();

 /**
  * Set the state of the real time flag to the new value
  *
  * @param NewValue True if the record is a real time record, otherwise false
  */
  public void setRealtime(boolean NewValue);

 /**
  * Is the IRecord valid. A valid record is defined as a real record (not a
  * control record, which are a different subject) which has no error that
  * inhibits normal processing.
  *
  * @return True if the record is valid, otherwise false
  */
  public boolean isValid();

 /**
  * Set the state of the valid flag to the new value
  *
  * @param NewValue The new value of the valid flag
  */
  public void setValid(boolean NewValue);

 /**
  * Is the IRecord going to be dumped because of having a dump flag set?
  *
  * @return True if the record is to be dumped, otherwise false
  */
  public boolean isDump();

 /**
  * Set the state of the dump flag to the new value. Setting this causes the
  * record to be dumped if the dump module is configured to dump selected
  * records.
  *
  * @param NewValue True if the record is to be dumped, otherwise false
  */
  public void setDump(boolean NewValue);

 /**
  * This tells us if any errors have been encountered during processing.
  * normally the record should have no errors, but in certain cases it will
  * fail some element of the processing and will have an error attached to it.
  * Depending on the type of error, the record may skip later processing
  * stages.
  *
  * @return True if the record has at least one error, otherwise false
  */
  public boolean isErrored();

 /**
  * Return the list of errors attached to this record.
  * Allows a cumulative process for attaching errors
  * to the record where the first error in the list is
  * the first error found during processing.
  *
  * returns the list of errors attached to this record.
  *
  * @return The list of errors
  */
  public List<IError> getErrors();

 /**
  * Return the number of errors attached to this record.
  *
  * @return the list of errors attached to this record.
  */
  public int getErrorCount();

 /**
  * Add error to this record
  *
  * @param error The error to add to this record
  */
  public void addError(IError error);

  /**
   * Clear the errors for this record
   *
   */
  public void clearErrors();

 /**
  * Return a String representation of this IRecord.
  *
  * @return The string representing the error
  */
  @Override
  public String toString();

 /**
  * Return a String representation of this IRecord in a format
  * suitable for dumping. To be overridden by each class that requires to
  * write to a dump.
  *
  * @return A list of strings which contains the dump information
  */
  public ArrayList<String> getDumpInfo();

 /**
  * Return whether the record should be written to a given output or not. The
  * output handling of multiple outputs means we can write a single record to
  * as many outputs as we wish. This function is usually called by an output
  * adapter with it's own configured name in order to see if the record
  * should be output in the current output adapter.
  *
  * @param OutputToCheck The name of the output to check
  * @return True if the output should be written to, otherwise false
  */
  public boolean getOutput(String OutputToCheck);

 /**
  * Return whether all outputs have been consumed or not, after deleting the
  * given output from the output list. When all outputs have been consumed, it
  * means the record can be dropped.
  *
  * @param OutputToDelete The output to consume
  * @param TerminatingAdapter True if the output adapter is the terminating adapter
  * @return True if there are no more outputs, otherwise false
  */
  public boolean deleteOutput(String OutputToDelete, boolean TerminatingAdapter);

 /**
  * This method allows the internal record ID to be set. This is primarily used
  * in real time processing, where we have to marshal the records which have
  * been processed to the same socket instance as they came in on.
  *
  * @param newRecordID The record ID to use
  */
  public void setRecordID(int newRecordID);

 /**
  * This method allows the internal record ID to be retrieved.
  *
  * @return The record ID of this record.
  */
  public int getRecordID();

  /**
   *
   * @return
   */
  public Object getCurrentStateObject();

  /**
   * 
   * @param currentStateObject
   */
  public void setCurrentStateObject(Object currentStateObject);

}
