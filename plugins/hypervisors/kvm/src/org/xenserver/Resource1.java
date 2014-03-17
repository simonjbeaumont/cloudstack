package org.xenserver;

import org.freedesktop.dbus.DBusInterface;

public interface Resource1 extends DBusInterface {

  public String attach(String global_uri, String owner_uri, String operation_id);
  public String detach(String id, String owner_uri, String operation_id);

}
