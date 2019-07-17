

package OpenRate.resource.notification;

import OpenRate.OpenRate;
import OpenRate.configurationmanager.ClientManager;
import OpenRate.configurationmanager.IEventInterface;
import OpenRate.exception.InitializationException;
import OpenRate.exception.ProcessingException;
import OpenRate.resource.IResource;
import OpenRate.utils.PropertyUtils;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;

/**
 * The email notification cache simplifies the process of sending email messages 
 * from OpenRate to alert in (Near) Real Time about things which might need
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
  private final String symbolicName = RESOURCE_KEY;

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

  // used to simplify logging and exception handling
  public String message;
  
  /**
   * constructor
   */
  public EmailNotificationCache()
  {
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
      message = "The linked buffer cache must be called " + RESOURCE_KEY;
      throw new InitializationException(message,getSymbolicName());
    }

    // Initialise the mail subsystem
    server = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"Server","None");
    if (server.equalsIgnoreCase("None"))
    {
      message = "Property <Server> not defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    String sPort = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"Port","None");
    if (sPort.equalsIgnoreCase("None"))
    {
      message = "Property <Port> not defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
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
      message = "Property <Port> was not numeric. Received: <" + sPort + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    System.out.println("    Port           <" + port + ">");

    userName = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"UserName","None");
    if (userName.equalsIgnoreCase("None"))
    {
      message = "Property <UserName> not defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      System.out.println("    User Name      <" + userName + ">");
    }

    passWord = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"PassWord","None");
    if (passWord.equalsIgnoreCase("None"))
    {
      message = "Property <PassWord> not defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    else
    {
      System.out.println("    Password set   <*******>");
    }

    mailTo = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailTo","None");
    if (mailTo.equalsIgnoreCase("None"))
    {
      message = "Property <mailTo> not defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
    }

    mailFrom = PropertyUtils.getPropertyUtils().getResourcePropertyValueDef(ResourceName,"MailFrom","None");
    if (mailFrom.equalsIgnoreCase("None"))
    {
      message = "Property <MailFrom> not defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
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
      message = "Invalid email address <MailFrom> defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    catch (UnsupportedEncodingException ex)
    {
      message = "Invalid email name <MailFromName> defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
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
      message = "Invalid email address <MailTo> defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
    }
    catch (UnsupportedEncodingException ex)
    {
      message = "Invalid email name <MailToName> defined for resource <" + ResourceName + ">";
      throw new InitializationException(message,getSymbolicName());
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
            CCAddresses[idx] = new InternetAddress(tmpCCAddresses[idx]);
          }
          else
          {
            CCAddresses[idx] = new InternetAddress(tmpCCAddresses[idx], carbonCopyName);
          }
        }
      }
      catch (AddressException ae)
      {
        message = "Invalid email address <MailTo> defined for resource <" + ResourceName + ">";
        throw new InitializationException(message,getSymbolicName());
      }
      catch (UnsupportedEncodingException ex)
      {
        message = "Invalid email name <MailToName> defined for resource <" + ResourceName + ">";
        throw new InitializationException(message,getSymbolicName());
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
      OpenRate.getOpenRateFrameworkLog().debug("Mail debugging turned on.");
      mailSession.setDebug(true);
    }

    // get the mailer
    emailer = new EmailerThread();

    // Pass the mail session (for authentication) down to the handler thread
    emailer.setMailSession(mailSession);

    // set the symbolic name into the thread
    emailer.setSymbolicName(getSymbolicName());

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
        message = "Failed to send startup email. Aborting.";
        throw new InitializationException(message,getSymbolicName());
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
      OpenRate.getOpenRateFrameworkLog().error("Error sending message");
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
      OpenRate.getOpenRateFrameworkLog().error("Error sending message");
      return false;
    }
      try {
        // Add the mail to the queue
        emailer.despatchEmailSync(msg);
      }
      catch (ProcessingException ex) {
        OpenRate.getFrameworkExceptionHandler().reportException(ex);
      }

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
      OpenRate.getOpenRateFrameworkLog().info("Waiting for emailer thread to finish. Still <" + emailer.getMessageCount() + "> mails to send.");
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
    ClientManager.getClientManager().registerClient("Resource",getSymbolicName(), this);

    //Register services for this Client
    ClientManager.getClientManager().registerClientService(getSymbolicName(), SERVICE_QUEUE_LENGTH, ClientManager.PARAM_DYNAMIC);
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
