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

package OpenRate.resource.notification;

import OpenRate.audit.AuditUtils;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.ExceptionHandler;
import OpenRate.exception.InitializationException;
import OpenRate.logging.ILogger;
import OpenRate.logging.LogUtil;
import OpenRate.resource.IResource;
import OpenRate.utils.PropertyUtils;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;

/**
 * The email notification cache simplifies the process of sending emails from
 * OpenRate to alert in (Near) Real Time about things which might need
 * urgent intervention, such as fraud detection.
 *
 * There are two modes of operation:
 *  - Asynchronous: This is the normal mode of operation, in which we send the
 *    mail in the separate mailer thread. This means that we do not hold up
 *    the processing in order to wait for the mail server. The mailer
 *    thread then sends the mails in the background.
 *
 *  - Synchronous: This sends a mail and blocks until the response is received.
 *    This is primarily intended for situations where we want to send a message
 *    from the framework, and in this case we can afford to do it the slow way.
 */
public class EmailNotificationCache implements IResource, IEventInterface
{
  // List of Services that this Client supports
  private final static String SERVICE_QUEUE_LENGTH = "MailQueueLength";

  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "EmailNotificationCache";

  // This is the symbolic name of the resource
  private String symbolicName = RESOURCE_KEY;

  // The mail address we are to despatch to
  private String mailTo;

  // The mail address we are to despatch copies to
  private String carbonCopy;

  // The mail address we are to send from
  private String mailFrom;

  // The mail session, with the parameters pre-configured
  private Session mailSession;

  private InternetAddress FromAddress;
  private InternetAddress[] ToAddresses;
  private InternetAddress[] CCAddresses;

  // Variables used to configure the mail server
  private String server;
  private int port;
  private String userName;
  private String passWord;
  private boolean SSLAuthentication = false;

  // Used to perform authenticated login
  private Authenticator authenticator = null;

  // The autonomous emailer thread
  private Thread emailerThread;
  private EmailerThread emailer;

  // Used to send the start and stop messages
  boolean notifyStartAndStop = false;

  // Used to identify the source of the notification
  String notificationInstanceID;

  // The parent Exception Handler
  private ExceptionHandler handler;

 /**
  * Access to the Framework AstractLogger. All non-pipeline specific messages (e.g.
  * from resources or caches) should go into this log, as well as startup
  * and shutdown messages. Normally the messages will be application driven,
  * not stack traces, which should go into the error log.
  */
  protected ILogger FWLog = LogUtil.getLogUtil().getLogger("Framework");

  /**
   * constructor
   */
  public EmailNotificationCache()
  {
    // Add ourselves to the version map
    AuditUtils.getAuditUtils().buildVersionMap(this.getClass());
  }

  /**
   * Perform whatever initialisation is required of the resource.
   * This method should only be called once per application instance.
   *
   * @param ResourceName The name of the resource in the properties
   */
  @Override
  public void init(String ResourceName) throws InitializationException
  {
    if (ResourceName.equals(RESOURCE_KEY) == false)
    {
      throw new InitializationException("The linked buffer cache must be called " + RESOURCE_KEY);
    }

    // Initialise the mail subsystem
    server = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"Server","None");
    if (server.equalsIgnoreCase("None"))
    {
      throw new InitializationException("Property <Server> not defined for resource <" + ResourceName + ">");
    }

    String sPort = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"Port","None");
    if (sPort.equalsIgnoreCase("None"))
    {
      throw new InitializationException("Property <Port> not defined for resource <" + ResourceName + ">");
    }

    // show some life
    System.out.println("    Mail server    <" + server + ">");

    // Convert the string value
    try
    {
      port = Integer.parseInt(sPort);
    }
    catch (NumberFormatException nfe)
    {
      throw new InitializationException("Property <Port> was not numeric. Received: <" + sPort + ">");
    }

    System.out.println("    Port           <" + port + ">");

    userName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"UserName","None");
    if (userName.equalsIgnoreCase("None"))
    {
      throw new InitializationException("Property <UserName> not defined for resource <" + ResourceName + ">");
    }
    else
    {
      System.out.println("    User Name      <" + userName + ">");
    }

    passWord = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"PassWord","None");
    if (passWord.equalsIgnoreCase("None"))
    {
      throw new InitializationException("Property <PassWord> not defined for resource <" + ResourceName + ">");
    }
    else
    {
      System.out.println("    Password set   <*******>");
    }

    mailTo = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailTo","None");
    if (mailTo.equalsIgnoreCase("None"))
    {
      throw new InitializationException("Property <mailTo> not defined for resource <" + ResourceName + ">");
    }

    mailFrom = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailFrom","None");
    if (mailFrom.equalsIgnoreCase("None"))
    {
      throw new InitializationException("Property <MailFrom> not defined for resource <" + ResourceName + ">");
    }

    // Create the mail from address
    try
    {
      String mailFromName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailFromName","None");
      if (mailFromName.equalsIgnoreCase("None"))
      {
        FromAddress = new InternetAddress(mailFrom);
      }
      else
      {
        FromAddress = new InternetAddress(mailFrom, mailFromName);
      }
    }
    catch (AddressException ae)
    {
      throw new InitializationException("Invalid email address <MailFrom> defined for resource <" + ResourceName + ">");
    }
    catch (UnsupportedEncodingException ex)
    {
      throw new InitializationException("Invalid email name <MailFromName> defined for resource <" + ResourceName + ">");
    }

    // Create the default mail to address
    try
    {
      String mailToName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailToName","None");

      // manage multiple addresses
      String []tmpToAddresses = mailTo.split(";|,");
      ToAddresses = new InternetAddress[tmpToAddresses.length];

      for (int idx = 0 ; idx < tmpToAddresses.length ; idx++ )
      {
        if (mailToName.equalsIgnoreCase("None"))
        {
          ToAddresses[idx] = new InternetAddress(tmpToAddresses[idx]);
        }
        else
        {
          ToAddresses[idx] = new InternetAddress(tmpToAddresses[idx], mailToName);
        }
      }
    }
    catch (AddressException ae)
    {
      throw new InitializationException("Invalid email address <MailTo> defined for resource <" + ResourceName + ">");
    }
    catch (UnsupportedEncodingException ex)
    {
      throw new InitializationException("Invalid email name <MailToName> defined for resource <" + ResourceName + ">");
    }

    // Get the optional CC address
    carbonCopy = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailCC","None");
    if (carbonCopy.equalsIgnoreCase("None") == false)
    {
      // Create the default mail to address
      try
      {
        String carbonCopyName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailCCName","None");

        String []tmpCCAddresses = carbonCopy.split(";|,");
        CCAddresses = new InternetAddress[tmpCCAddresses.length];

        for (int idx = 0 ; idx < tmpCCAddresses.length ; idx++ )
        {
          if (carbonCopyName.equalsIgnoreCase("None"))
          {
            CCAddresses[0] = new InternetAddress(tmpCCAddresses[idx]);
          }
          else
          {
            CCAddresses[0] = new InternetAddress(tmpCCAddresses[idx], carbonCopyName);
          }
        }
      }
      catch (AddressException ae)
      {
        throw new InitializationException("Invalid email address <MailTo> defined for resource <" + ResourceName + ">");
      }
      catch (UnsupportedEncodingException ex)
      {
        throw new InitializationException("Invalid email name <MailToName> defined for resource <" + ResourceName + ">");
      }
    }

    Properties props = System.getProperties();
    props.put("mail.smtp.host", server);
    props.put("mail.smtp.port", port);

    if (userName != null && userName.length() > 0)
    {
      props.put("mail.smtp.auth", "true");
      authenticator = new SMTPAuthenticator();
    }

    SSLAuthentication = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"SSLAuthentication","None").equalsIgnoreCase("true");
    if (SSLAuthentication)
    {
      props.put("mail.smtp.starttls.enable", "true");
      props.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
    }

    // get the instance id so we know who is sending
    notificationInstanceID = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"NotificationID","");
    System.out.println("    Instance ID    <" + notificationInstanceID + ">");

    mailSession = Session.getInstance(props, authenticator);

    String debugMail = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailDebug","False");

    Boolean debug = (debugMail.equalsIgnoreCase("true"));

    // extended debugging
    if(debug)
    {
      FWLog.debug("Mail debugging turned on.");
      mailSession.setDebug(true);
    }

    // get the mailer
    emailer = new EmailerThread();

    // will be handling threads
    emailer.setFWLog(FWLog);

    emailer.setMailSession(mailSession);

    // ToDo add exceptionhandler
    emailer.setHandler(getHandler());

    // start the mailer thread
    Thread emailerProcess = new Thread(emailer,"EmailNotificationThread");

    // Start the listener
    emailerProcess.start();

    // if we have a start mail configured, send it
    notifyStartAndStop = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"NotifyStartAndStop","false").equalsIgnoreCase("true");
    if(notifyStartAndStop)
    {
      System.out.println("    Sending startup notification mail");
      boolean sentOK = despatchMailInternalImmediate("EmailNotificationCacheStarted","The OpenRate email notification cache has started");

      if (sentOK == false)
      {
        // problems sending the mail - abort
        throw new InitializationException("Failed to send startup email. Aborting.");
      }
    }
  }

 /**
  * Add a mail to the mail queue for despatch. This sends to the default
  * email address. The sending of the mail is asynchronous.
  *
  * @param mailSubject the subject we will send the mail with
  * @param mailBody the message body we will send the mail with
  * @return true if the message was queued OK, otherwise false
  */
  public boolean despatchMailInternal(String mailSubject, String mailBody)
  {
    // set the subject for internal messages
    if (notificationInstanceID.isEmpty() == false)
    {
      mailSubject = "[" + notificationInstanceID + "] " + mailSubject;
    }

    // Fill in the blanks and send
    return despatchMailExternal(mailSubject,
                                mailBody,
                                FromAddress,
                                ToAddresses,
                                CCAddresses);
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
                                      InternetAddress fromAddress,
                                      InternetAddress[] toAddresses,
                                      InternetAddress[] ccAddresses)
  {
    MimeMessage msg;

    try
    {
      // create a message
      msg = new MimeMessage(mailSession);

      // ========= the from address (there can be only one) ========
      msg.setFrom(fromAddress);

      // ========= the to addresses (there can be more than one) ========
      msg.setRecipients(Message.RecipientType.TO, toAddresses);

      // ========= the CC addresses (there can be more than one) ========
      msg.setRecipients(Message.RecipientType.CC, ccAddresses);

      // ========= set the subject ========
      msg.setSubject(mailSubject);

      // ========= the body ========
      MimeBodyPart mbp1 = new MimeBodyPart();
      mbp1.setText(mailBody);

      // ========= the date ========
      msg.setSentDate(Calendar.getInstance().getTime());

      // create the Multipart and add its parts to it
      Multipart mp = new MimeMultipart();
      mp.addBodyPart(mbp1);

      // add the standard text
      msg.setContent(mp);
    }
    catch (MessagingException ex)
    {
      FWLog.error("Error sending message");
      return false;
    }

    // Add the mail to the queue
    emailer.queueMessage(msg);

    // Done!!!
    return true;
  }

 /**
  * Despatch a mail synchronously. This sends to the configured email address.
  * This is designed as a low volume process and as such we open a connection
  * each time we use it. This means that the sender waits for the mail to be
  * despatched before returning control.
  *
  * @param mailSubject the subject we will send the mail with
  * @param mailBody the message body we will send the mail with
  * @return true if the despatch was successful, otherwise false
  */
  public boolean despatchMailInternalImmediate(String mailSubject, String mailBody)
  {
    MimeMessage msg;

    try
    {
      // create a message
      msg = new MimeMessage(mailSession);

      // ========= the from address (there can be only one) ========
      msg.setFrom(FromAddress);

      // ========= the to addresses (there can be more than one) ========
      msg.setRecipients(Message.RecipientType.TO, ToAddresses);

      // ========= the CC addresses (there can be more than one) ========
      msg.setRecipients(Message.RecipientType.CC, CCAddresses);

      // ========= set the subject ========
      if (notificationInstanceID.isEmpty())
      {
        msg.setSubject(mailSubject);
      }
      else
      {
        msg.setSubject("[" + notificationInstanceID + "] " + mailSubject);
      }

      // ========= the body ========
      MimeBodyPart mbp1 = new MimeBodyPart();
      mbp1.setText(mailBody);

      // ========= the date ========
      msg.setSentDate(Calendar.getInstance().getTime());

      // create the Multipart and add its parts to it
      Multipart mp = new MimeMultipart();
      mp.addBodyPart(mbp1);

      // add the standard text
      msg.setContent(mp);
    }
    catch (MessagingException ex)
    {
      FWLog.error("Error sending message");
      return false;
    }

    // Add the mail to the queue
    emailer.despatchEmailSync(msg);

    // Done!!!
    return true;
  }

// -----------------------------------------------------------------------------
// -------------------- Start of local utility functions -----------------------
// -----------------------------------------------------------------------------

  /**
   * Perform any required cleanup.
   */
  @Override
  public void close()
  {
    // if we have a start mail configured, send it
    if(notifyStartAndStop)
    {
      System.out.println("    Sending shutdown notification mail");
      despatchMailInternalImmediate("EmailNotificationCacheStopped","The OpenRate email notification cache has stopped");
    }

    System.out.println("  Stopping Mailer resource.");

    // mark for closedown and wait for mailer to clear the queue
    emailer.markForClosedown();
    while (emailerThread != null &&  emailerThread.isAlive())
    {
      FWLog.info("Waiting for emailer thread to finish. Still <" + emailer.getMessageCount() + "> mails to send.");
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException ex)
      {
      }
    }
  }

 /**
  * Return the resource symbolic name
  */
  @Override
  public String getSymbolicName()
  {
    return symbolicName;
  }

  /**
   * Get the exception handler.
   *
   * @return the handler
   */
  public ExceptionHandler getHandler() {
    return handler;
  }

  /**
   * Set the exception handler for handling any exceptions.
   *
   * @param handler the handler to set
   */
  @Override
  public void setHandler(ExceptionHandler handler) {
    this.handler = handler;
  }

 /**
  * The JavaMail authenticator object. Needed for cases where we need to perform
  * authenticated logins to be able to send mails.
  */
  private class SMTPAuthenticator extends javax.mail.Authenticator
  {
    @Override
    public PasswordAuthentication getPasswordAuthentication()
    {
      return new PasswordAuthentication(userName, passWord);
    }
  }

  // -----------------------------------------------------------------------------
  // ------------- Start of inherited IEventInterface functions ------------------
  // -----------------------------------------------------------------------------

 /**
  * registerClientManager registers the client module to the ClientManager class
  * which manages all the client modules available in this OpenRate Application.
  *
  * registerClientManager registers this class as a client of the ECI listener
  * and publishes the commands that the plug in understands. The listener is
  * responsible for delivering only these commands to the plug in.
  *
  */
  @Override
  public void registerClientManager() throws InitializationException
  {
    //Register this Client
    ClientManager.registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.registerClientService(getSymbolicName(), SERVICE_QUEUE_LENGTH, ClientManager.PARAM_DYNAMIC);
  }

  /**
  * processControlEvent is the event processing hook for the External Control
  * Interface (ECI). This allows interaction with the external world, for
  * example turning the dumping on and off.
  */
  @Override
  public String processControlEvent(String Command, boolean Init, String Parameter)
  {

    int ResultCode = -1;

    if (Command.equalsIgnoreCase(SERVICE_QUEUE_LENGTH))
    {
      return Integer.toString(emailer.getMessageCount());
    }

    // Currently this cannot handle any dynamic events
    if (ResultCode == 0)
    {
        return "OK";
    }
    else
    {
      return "Command Not Understood";
    }
  }
}
