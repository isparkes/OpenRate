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

package OpenRate.utils;

import java.awt.*;
import java.awt.event.*;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import javax.swing.*;
import javax.swing.text.*;

/**
 * Process viewer id a GUI for viewing the internal processing of the OpenRate
 * framework.
 *
 * @author afzaal
 */
public class ORProcessViewer implements Runnable
{
   private final static int NULL = 0;
   private final static int DISCONNECTED = 1;
   private final static int DISCONNECTING = 2;
   private final static int BEGIN_CONNECT_SERVER = 3;
   private final static int BEGIN_CONNECT_CLIENT = 4;
   private final static int CONNECTED = 5;

   public final static String statusMessages[] = {
      " Unable to Connect, please check your network or firewall settings!", " Disconnected",
      " Disconnecting...", " Waiting for Client...",
      " Trying to Connect to Server....", " Connected"
   };
   public final static ORProcessViewer tcpObj = new ORProcessViewer();
   private final static String END_CHAT_SESSION = new Character((char)0).toString();

   private static String hostIP = "localhost";
   private static int port = 8208;
   private static int connectionStatus = DISCONNECTED;
   private static boolean isHost = false;
   private static String statusString = statusMessages[connectionStatus];
   private static StringBuffer toAppend = new StringBuffer("");
   private static StringBuffer toSend = new StringBuffer("");

   public static JFrame mainFrame = null;
   public static JTextPane chatText = null;
   public static StyledDocument docTextArea = null;
   public static JTextField chatLine = null;
   public static JPanel statusBar = null;
   public static JLabel statusField = null;
   public static JTextField statusColor = null;
   public static JTextField ipField = null;
   public static JTextField portField = null;
   public static JRadioButton hostOption = null;
   public static JRadioButton guestOption = null;
   public static JButton connectButton = null;
   public static JButton disconnectButton = null;

   public static ServerSocket hostServer = null;
   public static Socket socket = null;
   public static DataInputStream in = null;
   public static PrintWriter out = null;
   private static final String TEXT_LINE_TERMINATOR = "\n\n";
   private static Boolean toggledColor = false;
   public static final String[] initStyles =  { "regular_blue", "italic_blue",
                                                "bold_blue", "regular_black",
                                                "italic_black", "bold_black"   };

   private final static int REGULAR_BLUE = 0;
   private final static int ITALIC_BLUE = 1;
   private final static int BOLD_BLUE = 2;
   private final static int REGULAR_BLACK = 3;
   private final static int ITALIC_BLACK = 4;
   private final static int BOLD_BLACK = 5;

   private static JPanel initOptionsPane() {
      JPanel pane;
      ActionAdapter buttonListener;
      JPanel optionsPane = new JPanel(new GridLayout(4, 1));
      pane = new JPanel(new FlowLayout(FlowLayout.LEFT));
      pane.add(new JLabel("Host IP:"));
      ipField = new JTextField(10); ipField.setText(hostIP);
      ipField.setEnabled(false);
      ipField.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
               ipField.selectAll();
               if (connectionStatus != DISCONNECTED) {
                  changeStatusNTS(NULL, true);
               }
               else {
                  hostIP = ipField.getText();
               }
            }
         });
      pane.add(ipField);
//      optionsPane.add(pane);
//      pane = new JPanel(new FlowLayout(FlowLayout.LEFT));
      pane.add(new JLabel("Port:"));
      portField = new JTextField(10); portField.setEditable(true);
      portField.setText((new Integer(port)).toString());
      portField.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
               if (connectionStatus != DISCONNECTED) {
                  changeStatusNTS(NULL, true);
               }
               else {
                  int temp;
                  try {
                     temp = Integer.parseInt(portField.getText());
                     port = temp;
                  }
                  catch (NumberFormatException nfe) {
                     portField.setText((new Integer(port)).toString());
                     mainFrame.repaint();
                  }
               }
            }
         });
      pane.add(portField);
//      optionsPane.add(pane);

      buttonListener = new ActionAdapter() {
            @Override
            public void actionPerformed(ActionEvent e) {
               if (connectionStatus != DISCONNECTED) {
                  changeStatusNTS(NULL, true);
               }
               else {
                  isHost = e.getActionCommand().equals("host");
                  if (isHost) {
                     ipField.setEnabled(false);
                     ipField.setText("localhost");
                     hostIP = "localhost";
                  }
                  else {
                     ipField.setEnabled(true);
                  }
               }
            }
         };
      ButtonGroup bg = new ButtonGroup();
      hostOption = new JRadioButton("Server", false);
      hostOption.setMnemonic(KeyEvent.VK_H);
      hostOption.setActionCommand("host");
//      hostOption.addActionListener(buttonListener);
      guestOption = new JRadioButton("Guest", true);
      guestOption.setMnemonic(KeyEvent.VK_G);
      guestOption.setActionCommand("Client");
      guestOption.addActionListener(buttonListener);
      bg.add(hostOption);
      bg.add(guestOption);
//      pane = new JPanel(new GridLayout(1, 2));
//      pane.add(hostOption);
      pane.add(guestOption);
//      optionsPane.add(pane);
//      JPanel buttonPane = new JPanel(new GridLayout(1, 2));
      buttonListener = new ActionAdapter() {
            @Override
            public void actionPerformed(ActionEvent e) {
               if (e.getActionCommand().equals("connect")) {
            	  if(isHost) {
            		  changeStatusNTS(BEGIN_CONNECT_SERVER, true);
            	  }else {
            		  changeStatusNTS(BEGIN_CONNECT_CLIENT, true);
            	  }
               }
               // Disconnect
               else {
                  changeStatusNTS(DISCONNECTING, true);
               }
            }
         };
      connectButton = new JButton("Connect");
      connectButton.setMnemonic(KeyEvent.VK_C);
      connectButton.setActionCommand("connect");
      connectButton.addActionListener(buttonListener);
      connectButton.setEnabled(true);
      disconnectButton = new JButton("Disconnect");
      disconnectButton.setMnemonic(KeyEvent.VK_D);
      disconnectButton.setActionCommand("disconnect");
      disconnectButton.addActionListener(buttonListener);
      disconnectButton.setEnabled(false);
//      buttonPane.add(connectButton);
//      buttonPane.add(disconnectButton);
      pane.add(connectButton);
      pane.add(disconnectButton);
      optionsPane.add(pane);

      return optionsPane;
   }

   private static void initGUI() {
      statusField = new JLabel();
      statusField.setText(statusMessages[DISCONNECTED]);
      statusColor = new JTextField(1);
      statusColor.setBackground(Color.red);
      statusColor.setEditable(false);
      statusBar = new JPanel(new BorderLayout());
      statusBar.add(statusColor, BorderLayout.WEST);
      statusBar.add(statusField, BorderLayout.CENTER);
      JPanel optionsPane = initOptionsPane();
      JPanel chatPane = new JPanel(new BorderLayout());
      chatText = new JTextPane();
      docTextArea = (StyledDocument)chatText.getDocument();
      addStylesToDocument(docTextArea);
//      chatText = new JTextArea(10, 20);
//      chatText.setLineWrap(true);
      chatText.setEditable(false);
      chatText.setForeground(Color.blue);
      JScrollPane chatTextPane = new JScrollPane(chatText,
         JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
         JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
      chatLine = new JTextField();
      chatLine.setEnabled(false);
      chatLine.addActionListener(new ActionAdapter() {
            @Override
            public void actionPerformed(ActionEvent e) {
               String s = chatLine.getText();
               if (!s.equals("")) {
                  appendToChatBox(s + TEXT_LINE_TERMINATOR);
                  chatLine.selectAll();
                  sendString(s);
               }
            }
         });
      chatPane.add(chatLine, BorderLayout.SOUTH);
      chatPane.add(chatTextPane, BorderLayout.CENTER);
      chatPane.setPreferredSize(new Dimension(200, 200));
      JPanel mainPane = new JPanel(new BorderLayout());
      mainPane.add(statusBar, BorderLayout.SOUTH);
      mainPane.add(optionsPane, BorderLayout.NORTH);
      mainPane.add(chatPane, BorderLayout.CENTER);
      mainFrame = new JFrame("Process State Viewer");
      mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      mainFrame.setContentPane(mainPane);
      Toolkit tk = Toolkit.getDefaultToolkit();
      int xSize = ((int) tk.getScreenSize().getWidth());
      int ySize = ((int) tk.getScreenSize().getHeight());
      mainFrame.setSize(xSize, ySize);
      mainFrame.setVisible(true);
   }

   private static void changeStatusTS(int newConnectStatus, boolean noError) {
      if (newConnectStatus != NULL) {
         connectionStatus = newConnectStatus;
      }
      if (noError) {
         statusString = statusMessages[connectionStatus];
      }
      else {
         statusString = statusMessages[NULL];
      }
      SwingUtilities.invokeLater(tcpObj);
   }

   private static void changeStatusNTS(int newConnectStatus, boolean noError) {
      if (newConnectStatus != NULL) {
         connectionStatus = newConnectStatus;
      }
      if (noError) {
         statusString = statusMessages[connectionStatus];
      }
      else {
         statusString = statusMessages[NULL];
      }
      tcpObj.run();
   }

   private static void appendToChatBox(String s) {
      synchronized (toAppend) {
         toAppend.append(s);
      }
   }
   private static void sendString(String s) {
      synchronized (toSend) {
         toSend.append(s).append(TEXT_LINE_TERMINATOR);
      }
   }

   private static boolean toggleColor() {
      synchronized (toggledColor) {
          if(toggledColor.booleanValue())
            toggledColor = Boolean.FALSE;
          else
            toggledColor = Boolean.TRUE;
      }
      return toggledColor.booleanValue();
   }

   private static void cleanUp() {
      try {
         if (hostServer != null) {
            hostServer.close();
            hostServer = null;
         }
      }
      catch (IOException e) { hostServer = null; }

      try {
         if (socket != null) {
            socket.close();
            socket = null;
         }
      }
      catch (IOException e) { socket = null; }

      try {
         if (in != null) {
            in.close();
            in = null;
         }
      }
      catch (IOException e) { in = null; }

      if (out != null) {
         out.close();
         out = null;
      }
   }

  @Override
   public void run() {
      switch (connectionStatus) {
      case DISCONNECTED:
         connectButton.setEnabled(true);
         disconnectButton.setEnabled(false);
         ipField.setEnabled(true);
         portField.setEnabled(true);
         hostOption.setEnabled(true);
         guestOption.setEnabled(true);
         chatLine.setText(""); chatLine.setEnabled(false);
         statusColor.setBackground(Color.red);
         break;

      case DISCONNECTING:
         connectButton.setEnabled(false);
         disconnectButton.setEnabled(false);
         ipField.setEnabled(false);
         portField.setEnabled(false);
         hostOption.setEnabled(false);
         guestOption.setEnabled(false);
         chatLine.setEnabled(false);
         statusColor.setBackground(Color.orange);
         break;

      case CONNECTED:
         connectButton.setEnabled(false);
         disconnectButton.setEnabled(true);
         ipField.setEnabled(false);
         portField.setEnabled(false);
         hostOption.setEnabled(false);
         guestOption.setEnabled(false);
         chatLine.setEnabled(true);
         statusColor.setBackground(Color.green);
         break;

      case BEGIN_CONNECT_SERVER:
         connectButton.setEnabled(false);
         disconnectButton.setEnabled(false);
         ipField.setEnabled(false);
         portField.setEnabled(false);
         hostOption.setEnabled(false);
         guestOption.setEnabled(false);
         chatLine.setEnabled(false);
         chatLine.grabFocus();
         statusColor.setBackground(Color.orange);
         break;

       case BEGIN_CONNECT_CLIENT:
          connectButton.setEnabled(false);
          disconnectButton.setEnabled(false);
          ipField.setEnabled(false);
          portField.setEnabled(false);
          hostOption.setEnabled(false);
          guestOption.setEnabled(false);
          chatLine.setEnabled(false);
          chatLine.grabFocus();
          statusColor.setBackground(Color.orange);
          break;

      }

      ipField.setText(hostIP);
      portField.setText((new Integer(port)).toString());
      hostOption.setSelected(isHost);
      guestOption.setSelected(!isHost);
      statusField.setText(statusString);
      try {
    	  if(toggleColor()) {
    		  docTextArea.insertString(docTextArea.getLength(), toAppend.toString(), docTextArea.getStyle(initStyles[ITALIC_BLUE]));
    	  }else {
    		  docTextArea.insertString(docTextArea.getLength(), toAppend.toString(), docTextArea.getStyle(initStyles[ITALIC_BLUE]));
    	  }
      } catch (BadLocationException e) {
		    e.printStackTrace();
      }
      toAppend.setLength(0);

      mainFrame.repaint();
   }

   /**
    *
    * @param args
    */
   public static void main(String args[]) {
      String s;

      initGUI();

      while (true) {
         try {
            Thread.sleep(10);
         }
         catch (InterruptedException e) {}

         switch (connectionStatus) {
         case BEGIN_CONNECT_SERVER:
            try {
                  hostServer = new ServerSocket(port);
                  socket = hostServer.accept();
                  in = new DataInputStream(socket.getInputStream());
                  out = new PrintWriter(socket.getOutputStream(), true);
                  changeStatusTS(CONNECTED, true);
            }
            catch (IOException e) {
               cleanUp();
               changeStatusTS(DISCONNECTED, false);
            }
            break;

         case BEGIN_CONNECT_CLIENT:
             try {
                   socket = new Socket(hostIP, port);
                   in = new DataInputStream(socket.getInputStream());
                   out = new PrintWriter(socket.getOutputStream(), true);
                   changeStatusTS(CONNECTED, true);
             }
             catch (IOException e) {
                cleanUp();
                changeStatusTS(DISCONNECTED, false);
             }
             break;

         case CONNECTED:
            try {
               if (toSend.length() != 0) {
                  out.print(toSend); out.flush();
                  toSend.setLength(0);
                  changeStatusTS(NULL, true);
               }
                  s = in.readUTF();
                  if ((s != null) &&  (s.length() != 0)) {
                     if (s.equals(END_CHAT_SESSION)) {
                        changeStatusTS(DISCONNECTING, true);
                     }
                     else {
                        appendToChatBox(s + TEXT_LINE_TERMINATOR);
                        changeStatusTS(NULL, true);
                     }
                  }
            }
            catch (IOException e) {
               cleanUp();
               changeStatusTS(DISCONNECTED, false);
            }
            break;

         case DISCONNECTING:
            out.print(END_CHAT_SESSION); out.flush();
            cleanUp();
            changeStatusTS(DISCONNECTED, true);
            break;

         default: break;
         }
      }
   }

   /**
    * To Display text in styles
    *
    * @param doc
    */
   private static void addStylesToDocument(StyledDocument doc) {
       //Initialize some styles.
       Style style = StyleContext.getDefaultStyleContext().
                       getStyle(StyleContext.DEFAULT_STYLE);

       Style regular = doc.addStyle("regular", style);
       StyleConstants.setFontFamily(style, "SansSerif");

       Style regular_blue = doc.addStyle("regular_blue", regular);
       StyleConstants.setForeground(regular_blue, Color.blue);

       Style temp = doc.addStyle("italic_blue", regular_blue);
       StyleConstants.setItalic(temp, true);

       temp = doc.addStyle("bold_blue", regular_blue);
       StyleConstants.setBold(temp, true);

       Style regular_black = doc.addStyle("regular_black", regular);
       StyleConstants.setForeground(regular_black, Color.black);

       temp = doc.addStyle("italic_black", regular_black);
       StyleConstants.setItalic(temp, true);

       temp = doc.addStyle("bold_black", regular_black);
       StyleConstants.setBold(temp, true);


//       s = doc.addStyle("small", regular);
//       StyleConstants.setFontSize(s, 10);
//
//       s = doc.addStyle("large", regular);
//       StyleConstants.setFontSize(s, 16);
//
//       s = doc.addStyle("icon", regular);
//       StyleConstants.setAlignment(s, StyleConstants.ALIGN_LEFT);
//       ImageIcon pigIcon = createImageIcon("images/Pig.gif",
//                                           "a cute pig");
//       if (pigIcon != null) {
//           StyleConstants.setIcon(s, pigIcon);
//       }
//		 Commented for future use
//       s = doc.addStyle("button", regular);
//       StyleConstants.setAlignment(s, StyleConstants.ALIGN_CENTER);
//       ImageIcon soundIcon = createImageIcon("images/sound.gif",
//                                             "sound icon");
//       JButton button = new JButton();
//       if (soundIcon != null) {
//           button.setIcon(soundIcon);
//       } else {
//           button.setText("BEEP");
//       }
//       button.setCursor(Cursor.getDefaultCursor());
//       button.setMargin(new Insets(0,0,0,0));
//       button.setActionCommand(buttonString);
//       button.addActionListener(this);
//       StyleConstants.setComponent(s, button);
   }

   /**
    *
    * @param path
    * @param description
    * @return
    */
   private static ImageIcon createImageIcon(String path, String description) {
		java.net.URL imgURL = ORProcessViewer.class.getResource(path);
		if (imgURL != null) {
			return new ImageIcon(imgURL, description);
		} else {
			System.err.println("Couldn't find file: " + path);
			return null;
		}
	}


}
class ActionAdapter implements ActionListener {
  @Override
	public void actionPerformed(ActionEvent e) {}
}