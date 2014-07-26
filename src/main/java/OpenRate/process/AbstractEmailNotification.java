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
