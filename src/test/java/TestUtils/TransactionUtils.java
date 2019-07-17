
package TestUtils;

import OpenRate.exception.InitializationException;
import OpenRate.process.AbstractPlugIn;
import OpenRate.record.HeaderRecord;
import OpenRate.record.TrailerRecord;
import OpenRate.transaction.ITransactionManager;
import OpenRate.transaction.TransactionManagerFactory;
import org.junit.Assert;

/**
 *
 * @author TGDSPIA1
 */
public class TransactionUtils
{
  // Used for logging and exception handling
  private static String message; 

  public static ITransactionManager getTM()
  {
      ITransactionManager TM = null;
      
      try {
        // Create a transaction manager for the test pipe
        TM = TransactionManagerFactory.getTransactionManager("DBTestPipe");
      } catch (InitializationException ex) {
        message = "Error getting transaction manager in <AbstractDuplicateCheckTest>";
        Assert.fail(message);
      }

    return TM;
  }

 /**
  * Simulate the start of a transaction by creating a transaction in the
  * transaction manager and pushing a header record into the module. The header
  * record triggers the internal processing in the module.
  *
  * @param TM
  */
  public static int startTransactionPlugIn(AbstractPlugIn instance)
  {
    int transNumber;

    // Start a transaction
    ITransactionManager TM = TransactionUtils.getTM();
    transNumber = TM.openTransaction("test");

    // Create a header record to start the transaction management in the module
    HeaderRecord tmpHDR = new HeaderRecord();
    tmpHDR.setTransactionNumber(transNumber);
    instance.procHeader(tmpHDR);

    return transNumber;
  }

  public static int endTransactionPlugIn(AbstractPlugIn instance, int ourTransNumber)
  {
    int transNumber = ourTransNumber;

    // Create a trailer record to stop the transaction management in the module
    TrailerRecord tmpTLR = new TrailerRecord();
    tmpTLR.setTransactionNumber(transNumber);
    instance.procTrailer(tmpTLR);

    transNumber = 0;

    return transNumber;
  }

  public static int getOpenTransactionCount()
  {
    // Create a trailer record to stop the transaction management in the module
    ITransactionManager TM = TransactionUtils.getTM();
    return TM.getActiveTransactionCount() + TM.getFlushedTransactionCount();
  }


}
