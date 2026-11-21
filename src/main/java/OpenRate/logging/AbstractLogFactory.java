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

import OpenRate.exception.InitializationException;
import OpenRate.utils.ClassUtils;
import OpenRate.resource.IResource;

/**
 * AbstractLogFactory - create Factory for a certain logging library.
 */
public abstract class AbstractLogFactory implements IResource
{
  /**
   * This is the key name we will use for referencing this object from the
   * Resource context
   */
  public static final String RESOURCE_KEY = "LogFactory";

  /**
   * Get a logger instance for the provided type.
   *
   * @param type The logger type
   * @return The logger
   */
  public abstract AstractLogger getLogger(String type);

  /**
   * Get a logger instance for the provided type.
   *
   * @param type The logger type
   * @return The logger
   * @throws InitializationException
   */
  public AstractLogger getLogger(Class<?> type) throws InitializationException
  {
    return getLogger(type.getName());
  }

  /**
   * Get a AbstractLogFactory based on the implementation class name
   * provided as a parameter.
   *
   * @param factoryImpl
   * @return The log factory
   * @throws InitializationException
   */
  public static AbstractLogFactory getFactory(String factoryImpl)
    throws InitializationException
  {
    AbstractLogFactory factory = null;

    try
    {
      if (factoryImpl == null)
      {
        throw new InitializationException("LogFactory className == null",RESOURCE_KEY);
      }

      Class<?> type = Class.forName(factoryImpl);
      Object   obj  = ClassUtils.newInstance(type);
      factory     = (AbstractLogFactory) obj;
    }
    catch (ClassCastException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "LogFactory class name is not a LogFactory sub-class.",ex,RESOURCE_KEY);
    }
    catch (ClassNotFoundException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "LogFactory implementation class not found in classpath.",ex,RESOURCE_KEY);
    }
    catch (ReflectiveOperationException ex)
    {
      throw new InitializationException("LogFactory.getFactory(): " +
        "Could not create the LogFactory. Check that it has a public " +
        "no-argument constructor. <" + ClassUtils.describeFailure(ex) + ">",
        ex,RESOURCE_KEY);
    }

    return factory;
  }
}
