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

package OpenRate.transaction;

/**
 * This class holds the definitions for the transaction manager.
 * 
 * @author tgdspia1
 */
public class TMDefs
{
  private static TMDefs tmDef = new TMDefs();
  
  /**
   * This is the default creation state, and is the equivalent of a null value,
   * meaning only that the new transaction object has been created
   */
  public final int TM_NONE          = 0;

  /**
   * The transaction is in a state of processing
   */
  public final int TM_PROCESSING    = 1;

  /**
   * All of the input of the transaction has been read, but we have not yet
   * finished all the output
   */
  public final int TM_FLUSHED       = 2;

  /**
   * All of the processing and output has finished, but there was some error
   * during processing. This will normally lead to an abort (rollback)
   */
  public final int TM_FINISHED_ERR  = 3;

  /**
   * All of the processing and output has finished, and there was no error
   * during processing. This will normally lead to a commit
   */
  public final int TM_FINISHED_OK   = 4;

  /**
   * The transaction has finished up, and any closure work in the module can be 
   * done.
   */
  public final int TM_CLOSING       = 5;
  
  /**
   * The transaction has closed and can be removed.
   */
  public final int TM_CLOSED        = 6;
  
 /**
  * This value defines that the registered client is an input adapter
  */
  public final int CT_CLIENT_INPUT         = 1;

 /**
  * This value defines that the registered client is a processing adapter
  */
  public final int CT_CLIENT_PROC          = 2;

 /**
  * This value defines that the registered client is an output adapter
  */
  public final int CT_CLIENT_OUTPUT        = 3;

 /**
  * This value defines that the registered client is a data cache
  */
  public final int CT_DATA_CACHE           = 4;
  
  /**
   * This class
   * 
   * @return
   */
  public static TMDefs getTMDefs()
  {
    return tmDef;
  }
}
