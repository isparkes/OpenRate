

package OpenRate.process;

import OpenRate.exception.InitializationException;
import OpenRate.record.IRecord;
import OpenRate.resource.ResourceContext;
import OpenRate.resource.notification.EmailNotificationCache;
import OpenRate.utils.PropertyUtils;
import java.util.ArrayList;
import java.util.Iterator;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

/**
 * Email notification module.
 */
public abstract class AbstractEmailNotification
  extends AbstractPlugIn
{
  // get the Cache manager for the zone map
  // We assume that there is one cache manager for
  // the zone, time and service maps, just to simplify
  // the configuration a bit

  // The zone model object
  private EmailNotificationCache ENC;

  // -----------------------------------------------------------------------------
  // ------------------ Start of inherited Plug In functions ---------------------
  // -----------------------------------------------------------------------------

 /**
  * Initialise the module. Called during pipeline creation to initialise:
  *  - Configuration properties that are defined in the properties file.
  *  - The references to any cache objects that are used in the processing
  *  - The symbolic name of the module
  *
  * @param PipelineName The name of the pipeline this module is in
  * @param ModuleName The name of this module in the pipeline
  * @throws OpenRate.exception.InitializationException
  */
  @Override
  public void init(String PipelineName, String ModuleName)
            throws InitializationException
  {
    // Variable for holding the cache object name
    String CacheObjectName;

    super.init(PipelineName,ModuleName);

    // Get the cache object reference
    CacheObjectName = PropertyUtils.getPropertyUtils().getPluginPropertyValue(PipelineName,
                                                           ModuleName,
                                                           "DataCache");

    // Get access to the conversion cache
    ResourceContext ctx    = new ResourceContext();

    // get the reference to the buffer cache
    ENC = (EmailNotificationCache) ctx.get(CacheObjectName);

    if (ENC == null)
    {
      message = "Could not find cache entry for <" + CacheObjectName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
  }

 /**
  * This is called when the synthetic Header record is encountered, and has the
  * meaning that the stream is starting.
  */
  @Override
  public IRecord procHeader(IRecord r)
  {

    return r;
  }

 /**
  * This is called when the synthetic trailer record is encountered, and has the
  * meaning that the stream is now finished.
  */
  @Override
  public IRecord procTrailer(IRecord r)
  {

    return r;
  }

  // -----------------------------------------------------------------------------
  // -------------------- Start of custom Plug In functions ----------------------
  // -----------------------------------------------------------------------------

 /**
  * Queue a mail for asynchronous despatch. This sends to the predefined
  * internal email address.  The sending of the mail is asynchronous.
  *
  * @param Subject The subject for the mail
  * @param MailBody The mail body text
  * @return True if queued OK, otherwise false
  */
  public boolean despatchMailInternal(String Subject, String MailBody)
  {
    return ENC.despatchMailInternal(Subject, MailBody);
  }

 /**
  * Add a mail to the mail queue for despatch. This sends to the defined
  * external email address. The sending of the mail is asynchronous.
  *
  * @param mailSubject the subject we will send the mail with
  * @param mailBody the message body we will send the mail with
  * @param fromAddress the mail address that will be used for the mail
  * @param toAddresses list of addresses to be sent to
  * @param ccAddresses list of addresses that should get a copy
  * @return true if the message was queued OK, otherwise false
  */
  public boolean despatchMailExternal(String mailSubject,
                                      String mailBody,
                                      InternetAddress   fromAddress,
                                      InternetAddress[] toAddresses,
                                      InternetAddress[] ccAddresses)
  {
    return ENC.despatchMailExternal(mailSubject, mailBody, fromAddress, toAddresses, ccAddresses);
  }

 /**
  * Create an email ready email address out of a string. Utility for getting
  * the internal format email address.
  *
  * @param emailAddress The email address to prepare
  * @return The prepared email address
  */
  public InternetAddress prepareEmailAddress(String emailAddress)
  {
    InternetAddress newAddress = null;
    try
    {
      newAddress = new InternetAddress(emailAddress);
    }
    catch (AddressException ex)
    {
      getPipeLog().error("Could not prepare email address <" + emailAddress + ">. Message <" + ex.getMessage() + ">");
    }

    return newAddress;
  }

 /**
  * Create an email ready email address out of a string. Utility for getting
  * the internal format email address.
  *
  * @param emailAddressList The list of email addresses to prepare, comma separated
  * @return The prepared email address
  */
  public InternetAddress[] prepareEmailAddressList(ArrayList<String> emailAddressList)
  {
    ArrayList<InternetAddress> newAddressList = new ArrayList<>();
    Iterator<String> addressIter = emailAddressList.iterator();
    InternetAddress[] returnList;

    while (addressIter.hasNext())
    {
      String address = addressIter.next();
      newAddressList.add(prepareEmailAddress(address));
    }

    // Put into an array for the addresses that made it
    returnList = new InternetAddress[newAddressList.size()];
    int i = 0;

    Iterator<InternetAddress> newAddressIter = newAddressList.iterator();
    while (newAddressIter.hasNext())
    {
      returnList[i++] = newAddressIter.next();
    }

    return returnList;
  }
}
