package org.xenserver;

import java.util.List;
import org.freedesktop.dbus.DBusInterface;

public interface TaskOwner1 extends DBusInterface {

  public List<Boolean> ping(List<String> uris);

}
