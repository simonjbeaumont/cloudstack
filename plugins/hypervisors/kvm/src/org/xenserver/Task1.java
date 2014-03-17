package org.xenserver;

import org.freedesktop.dbus.DBusInterface;
import org.freedesktop.dbus.DBusSignal;
import org.freedesktop.dbus.exceptions.DBusException;

public interface Task1 extends DBusInterface {

   public static class Completed extends DBusSignal
   {
      public Completed(String path) throws DBusException
      {
         super(path);
      }
   }

  public void cancel();
  public void destroy();
  public String getResult();

}
