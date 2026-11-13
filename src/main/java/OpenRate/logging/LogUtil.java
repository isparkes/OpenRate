/*
 * Copyright 2006-2026 Ian Sparkes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package OpenRate.logging;

import OpenRate.OpenRate;
import OpenRate.exception.InitializationException;
import OpenRate.resource.ResourceContext;

/**
 * Helper method to creating AstractLogger instance.
 *
 */
public class LogUtil
{
  // Get the utilities for handling the XML configuration
  private static LogUtil logUtilsObj;

 /**
  * This utility function returns the singleton instance of LogUtils
  *
  * @return    the instance of PropertyUtils
  */
  public static LogUtil getLogUtil()
  {
    if(logUtilsObj == null)
    {
      logUtilsObj = new LogUtil();
    }

    return logUtilsObj;
  }

  /**
   * Get the appropriate AstractLogger instance for this category
   *
   * @param LoggerName The logger name to get
   * @return The logger
   */
  public ILogger getLogger(String LoggerName)
  {
    AstractLogger          logger = null;

    ResourceContext ctx    = new ResourceContext();

    // try the new Logging model.
    AbstractLogFactory factory = (AbstractLogFactory) ctx.get(AbstractLogFactory.RESOURCE_KEY);

    if (factory == null)
    {
      // no factory registered, error
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("No log factory found","LogUtil"));
    }
    else
    {
      logger = factory.getLogger(LoggerName);
    }

    if (logger == null)
    {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("unable to load logger. Resource not found","LogUtil"));
    }

    return logger;
  }

  /**
   * Get the appropriate AstractLogger instance for this category
   *
   * @param LoggerName The logger name to get
   * @return The logger
   */
  public static AstractLogger getStaticLogger(String LoggerName)
  {
    AstractLogger          logger = null;

    ResourceContext ctx    = new ResourceContext();

    // try the new Logging model.
    AbstractLogFactory factory = (AbstractLogFactory) ctx.get(AbstractLogFactory.RESOURCE_KEY);

    if (factory == null)
    {
      // no factory registered, error
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("No log factory found","LogUtil"));
    }
    else
    {
      logger = factory.getLogger(LoggerName);
    }

    if (logger == null)
    {
      OpenRate.getFrameworkExceptionHandler().reportException(new InitializationException("unable to load logger. Resource not found","LogUtil"));
    }

    return logger;
  }

  /**
   * This method allows failures to be logged, even if the failure
   * is in the logging package.
   *
   * @return The default logger
   */
  public AstractLogger getDefaultLogger()
  {
    return new DefaultLogger();
  }

 /**
  * Prepare a string for logging ECI commands from a pipe module
  *
  * @param SymbolicName The name of the module
  * @param PipeName The name of the pipe the module belongs to
  * @param Command The command that was requested
  * @param Parameter The parameter for the command
  * @return The compiled log string
  */
  public static String LogECIPipeCommand(String SymbolicName, String PipeName, String Command, String Parameter)
  {
    return "Command <" + Command + "> handled by <" + SymbolicName + "> in pipe <" + PipeName + "> with parameter <" + Parameter + ">";
  }

 /**
  * Prepare a string for logging ECI commands from a framework module
  *
  * @param Command The command that was requested
  * @param Parameter The parameter for the command
  * @return The compiled log string
  */
  public static String LogECIFWCommand(String Command, String Parameter)
  {
    return "Command <" + Command + "> handled by <Framework> with parameter <" + Parameter + ">";
  }

 /**
  * Prepare a string for logging ECI commands from a cache module
  *
  * @param SymbolicName The name of the module
  * @param Command The command that was requested
  * @param Parameter The parameter for the command
  * @return The compiled log string
  */
  public static String LogECICacheCommand(String SymbolicName, String Command, String Parameter)
  {
    return "Command <" + Command + "> handled by <" + SymbolicName + "> with parameter <" + Parameter + ">";
  }
}

