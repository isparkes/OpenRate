

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
