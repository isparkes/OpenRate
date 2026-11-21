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

package OpenRate.utils;

import java.lang.reflect.InvocationTargetException;

/**
 * Helpers for creating the objects that the configuration names by class.
 *
 * @author ian
 */
public class ClassUtils
{
 /**
  * Create an instance of a configured class, using its no-argument
  * constructor.
  *
  * This replaces Class.newInstance(), which is deprecated. The deprecation
  * is not the only reason to move: Class.newInstance() propagated whatever
  * checked exception the constructor threw without declaring it, so a module
  * that failed in its own constructor surfaced somewhere unrelated, with a
  * message that pointed at the wrong thing. getDeclaredConstructor()
  * .newInstance() wraps it in InvocationTargetException, which
  * describeFailure() below unpacks.
  *
  * @param theClass The class to create an instance of
  * @return The new instance
  * @throws ReflectiveOperationException if there is no usable no-argument
  *         constructor, or the constructor itself failed
  */
  public static Object newInstance(Class<?> theClass) throws ReflectiveOperationException
  {
    return theClass.getDeclaredConstructor().newInstance();
  }

 /**
  * Describe why a newInstance() call failed, in a form that can go into an
  * error message. A constructor that threw is reported wrapped in an
  * InvocationTargetException, which says nothing useful on its own, so
  * unwrap it and describe what actually went wrong.
  *
  * @param ex The exception that newInstance() threw
  * @return The description
  */
  public static String describeFailure(ReflectiveOperationException ex)
  {
    Throwable cause = ex;

    if ((ex instanceof InvocationTargetException) && (ex.getCause() != null))
    {
      cause = ex.getCause();
    }

    if (cause instanceof NoSuchMethodException)
    {
      return "no public no-argument constructor";
    }

    return cause.getClass().getSimpleName() + ": " + cause.getMessage();
  }
}
