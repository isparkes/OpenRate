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

import OpenRate.OpenRate;
import OpenRate.exception.ProcessingException;
import java.util.ArrayList;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;

/**
 * This module implements a socket server that listens on a TCP port for
 * incoming connections and instantiates a listener thread for each of the
 * connections requested. A maximum number of connections is defined. Any
 * connection that exceeds the maximum number will be refused.
 *
 * @author ian
 */
public class EmailerThread implements Runnable
{
  // True while we are running, set false to exit thread
  private boolean inLoop = true;

  // This is the mail queue - this allows asynchronous sending of messages
  private static ArrayList<MimeMessage> mailQueue = new ArrayList<>();

  // This is the mail session for the asynchronous despatch
  private Session mailSessionAsync;

  // This is the mail session for the synchronous despatch
  private Session mailSessionSync;
  
  // The symbolic name to use for reporting errors
  private String symbolicName;

  // used to simplify logging and exception handling
  public String message;
  
 /**
  * Constructor
  */
  public EmailerThread()
  {
  }

 /**
  * This thread serves as a self-managing email thread. The connection is
  * opened as required, and shuts down after a period of inactivity.
  *
  * For one off messages, use the synchronous despatch method.
  */
  @Override
  public void run()
  {
    MimeMessage tmpMessage;
    Transport smtpTransport = null;

    //while we are running or have a queue to clear
    while ((inLoop) | (mailQueue.size() > 0))
    {
      if (mailQueue.isEmpty())
      {
        try
        {
          // Just sleep
          Thread.sleep(1000);
        }
        catch (InterruptedException ex)
        {
          // Nop
        }
      }
      else
      {
        // Open the connection
        try
        {
          if (smtpTransport == null)
          {
            smtpTransport = mailSessionAsync.getTransport("smtp");
          }

          if (smtpTransport.isConnected() == false)
          {
            smtpTransport.connect();
          }

          OpenRate.getOpenRateFrameworkLog().debug("SMTP Connection opened.");
        }
        catch (NoSuchProviderException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("No provider for smtp", ex);
        }
        catch (MessagingException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("Messaging exception", ex);
        }

        // dequeue the message
        tmpMessage = mailQueue.get(0);
        mailQueue.remove(0);

        try
        {
          // Send a mail
          message = "Sending email [" + tmpMessage.getSubject() + "] addresses [" + formatMailAddresses(tmpMessage) + "]";

          OpenRate.getOpenRateFrameworkLog().info(message);
          smtpTransport.sendMessage(tmpMessage, tmpMessage.getAllRecipients());
        }
        catch (MessagingException ex)
        {
          OpenRate.getOpenRateFrameworkLog().error("Sending email failed. Message <" + ex.getMessage() + ">");
        }
      }
    }
  }

 /**
  * Send an email immediately. This blocks until the mail is sent. This is
  * designed as a low volume method, and as such it opens and closes the
  * connection on demand.
  *
  * @param tmpMessage The mail message to send
  */
  public void despatchEmailSync(MimeMessage tmpMessage) throws ProcessingException
  {
    Transport smtpTransport;

    // Open the connection
    try
    {
      smtpTransport = mailSessionSync.getTransport("smtp");
      smtpTransport.connect();

      OpenRate.getOpenRateFrameworkLog().debug("Immediate SMTP Connection opened.");

      // Send a mail
      message = "Sending email [" + tmpMessage.getSubject() + "] addresses [" + formatMailAddresses(tmpMessage) + "]";

      // Log the message
      OpenRate.getOpenRateFrameworkLog().info(message);

      // Do the sending
      smtpTransport.sendMessage(tmpMessage, tmpMessage.getAllRecipients());

      // Close the connection down
      smtpTransport.close();
      OpenRate.getOpenRateFrameworkLog().debug("Immediate SMTP Connection opened.");
    }
    catch (NoSuchProviderException ex)
    {
      message = "No provider for smtp";
      throw new ProcessingException(message,ex,getSymbolicName());
    }
    catch (MessagingException ex)
    {
    	message = "Messaging exception";
      throw new ProcessingException(message,ex,getSymbolicName());
    }
  }

  // -----------------------------------------------------------------------------
  // ----------------------- Start of utility functions --------------------------
  // -----------------------------------------------------------------------------

 /**
  * Shut down the listener thread
  */
  void markForClosedown()
  {
    // Break the loop for this socket
    inLoop = false;
  }

  void queueMessage(MimeMessage msg)
  {
    mailQueue.add(msg);
  }

  /**
   * Get the number of messages waiting for despatch
   *
   * @return The number of mails in the queue
   */
  public int getMessageCount()
  {
    return mailQueue.size();
  }

 /**
  * Set the mail session with all the parameters set ready for connection and
  * mail sending.
  *
  * @param newMailSession The pre-configured mail session
  */
  void setMailSession(Session newMailSession)
  {
    mailSessionAsync = newMailSession;
    mailSessionSync = newMailSession;
  }

  private String formatMailAddresses(MimeMessage msg)
  {
    String addressList = null;
    int addressCount;

    try
    {
      // Get the To Mail addresses
      if (msg.getRecipients(RecipientType.TO) != null)
      {
        addressCount = msg.getRecipients(RecipientType.TO).length;
        for (int i = 0 ; i < addressCount ; i++)
        {
          if (i > 0)
          {
            addressList = addressList + "," + msg.getRecipients(RecipientType.TO)[i].toString();
          }
          else
          {
            addressList = "TO: " + msg.getRecipients(RecipientType.TO)[i].toString();
          }
        }
      }

      // Get the To CC addresses
      if (msg.getRecipients(RecipientType.CC) != null)
      {
        addressCount = msg.getRecipients(RecipientType.CC).length;
        for (int i = 0 ; i < addressCount ; i++)
        {
          if (i > 0)
          {
            addressList = addressList + "," + msg.getRecipients(RecipientType.CC)[i].toString();
          }
          else
          {
            addressList = addressList + " CC: " + msg.getRecipients(RecipientType.CC)[i].toString();
          }
        }
      }
    } catch (MessagingException ex)
    {
      addressList = "Unknown";
    }

    return addressList;
  }

    void setSymbolicName(String newSymbolicName) {
      symbolicName = newSymbolicName;
    }

    /**
     * @return the symbolicName
     */
    public String getSymbolicName() {
        return symbolicName;
    }
}
