/* ====================================================================
 * Limited Evaluation License:
 *
 * This software is open source, but licensed. The license with this package
 * is an evaluation license, which may not be used for productive systems. If
 * you want a full license, please contact us.
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

package OpenRate.transaction;

/**
  * The transaction info holds the information for a given transaction.
 *
 * @author tgdspia1
 */
public class TransactionInfo
{
 /*
  */
  private int     transactionStatus ;               // The overall status of the transaction
                                                    // being the sum of the client statuses.
                                                    // This will become TM_PROCESSING when
                                                    // the first client starts processing
                                                    // and TM_FINISHED when the last module
                                                    // completes its work.
                                                    // An TM_ABORT_REQUEST from any of the clients
                                                    // will cause the status to change immediately to
                                                    // TM_ABORT_REQUEST, turning to TM_ABORTED when all
                                                    // modules confirm that they have aborted.
  private int[]   clientStatus       = new int[50]; // The status of the individual clients
  private int[]   recordCount        = new int[50]; // The record count from the clients
  private long    transactionStart   = 0;           // Transaction start time
  private long    transactionEnd     = 0;           // Transaction end time
  private int     transactionRecords = 0;           // Transaction record count
  private boolean abortRequested     = false;       // True if an abort has been requested for this transaction
  private boolean transactionErrored = false;       // True if there was a critical error in this transaction
  private boolean stateChange        = false;       // True if there was an overall state change as part of the last client status change
  private String  pipeline           = null;        // The pipeline that opened this transaction
  private int     transactionNumber  = 0;           // The number of this transaction

  /**
    * @return the TransactionStart
    */
  public long getTransactionStart() {
    return transactionStart;
  }

  /**
    * @param newTransactionStart the TransactionStart to set
    */
  public void setTransactionStart(long newTransactionStart) {
    transactionStart = newTransactionStart;
  }

  /**
    * @return the TransactionEnd
    */
  public long getTransactionEnd() {
    return transactionEnd;
  }

  /**
    * @param newTransactionEnd the TransactionEnd to set
    */
  public void setTransactionEnd(long newTransactionEnd) {
    transactionEnd = newTransactionEnd;
  }

  /**
    * @return the TransactionStatus
    */
  public int getTransactionStatus() {
    return transactionStatus;
  }

  /**
    * @param newTransactionStatus the TransactionStatus to set
    */
  public void setTransactionStatus(int newTransactionStatus) {
    transactionStatus = newTransactionStatus;
  }

  /**
    * @return the TransactionRecords
    */
  public int getTransactionRecords() {
    return transactionRecords;
  }

  /**
    * @param newTransactionRecords the TransactionRecords to set
    */
  public void setTransactionRecords(int newTransactionRecords) {
    transactionRecords = newTransactionRecords;
  }

  /**
    * @return the AbortRequested
    */
  public boolean isAbortRequested() {
    return abortRequested;
  }

  /**
   * Set the request abort flag. This will cause the rest of the transaction
   * to be skipped.
   *
   * @param newAbortRequested the new value to set
   */
  public void setAbortRequested(boolean newAbortRequested) {
    abortRequested = newAbortRequested;
  }

  /**
    * @return the TransactionErrored
    */
  public boolean isTransactionErrored() {
    return transactionErrored;
  }

  /**
   * Set the errored flag for the transaction. This will cause the transaction
   * to be errored out.
   *
   * @param newTransactionErrored
   */
  public void setTransactionErrored(boolean newTransactionErrored) {
    transactionErrored = newTransactionErrored;
  }

  /**
    * @return the stateChange
    */
  public boolean isStateChange() {
    return stateChange;
  }

  /**
    * @param stateChange the stateChange to set
    */
  public void setStateChange(boolean stateChange) {
    this.stateChange = stateChange;
  }

  /**
   * Get the status value for a client.
   *
   * @param clientNumber The client number to get the status for
   * @return the client status value
   */
  public int getClientStatus(int clientNumber) {
    return clientStatus[clientNumber];
  }

  /**
   * Set the status value for a client
   *
   * @param clientNumber the client number
   * @param newStatus the new status
   */
  public void setClientStatus(int clientNumber, int newStatus) {
    clientStatus[clientNumber] = newStatus;
  }

  /**
   * Get the record count for a client.
   *
   * @param clientNumber the client number
   * @return the recordCount
   */
  public int getRecordCount(int clientNumber) {
    return recordCount[clientNumber];
  }

  /**
   * Set the record count for a client.
   *
   * @param clientNumber
   * @param recordCount the recordCount to set
   */
  public void setRecordCount(int clientNumber, int recordCount) {
    this.recordCount[clientNumber] = recordCount;
  }

  /**
    * @return the pipeline
    */
  public String getPipeline() {
    return pipeline;
  }

  /**
    * @param pipeline the pipeline to set
    */
  public void setPipeline(String pipeline) {
    this.pipeline = pipeline;
  }

  /**
   * @return the transactionNumber
   */
  public int getTransactionNumber() {
    return transactionNumber;
  }

  /**
   * @param transactionNumber the transactionNumber to set
   */
  public void setTransactionNumber(int transactionNumber) {
    this.transactionNumber = transactionNumber;
  }
}
