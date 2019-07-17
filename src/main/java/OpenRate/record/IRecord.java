

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
