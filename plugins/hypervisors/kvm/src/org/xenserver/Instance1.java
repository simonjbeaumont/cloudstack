package org.xenserver;

import org.freedesktop.dbus.DBusInterface;

public interface Instance1 extends DBusInterface {

  public String start(String global_uri, String owner_uri, String operation_id);

}
