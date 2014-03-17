package org.xenserver;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import org.freedesktop.dbus.DBusConnection;
import org.freedesktop.dbus.exceptions.DBusException;
import org.xenserver.TaskOwner1;

/** A TaskOwner responds to 'ping' messages to verify which Tasks
 *  are still considered 'alive'. A Task object will only be
 *  automatically destroyed when the activity has completed and the
 *  TaskOwner fails to recognise it. This is similar to Unix where
 *  a terminated (zombie) process whose parent quits is reparented
 *  to 'init' which reaps it.
 */
public class TaskOwner implements TaskOwner1 {
  private DBusConnection conn;

  /** Help generate a unique object path per instance of this class
   *  by appending a unique integer to a prefix. */
  private static int counter = 0;
  private int getNextId() {
    synchronized(this) {
      int next = this.counter;
      this.counter = this.counter + 1;
      return next;
    }
  }
  public static final String prefix = "/org/xenserver/task/owner";

  /** Our uri, to be passed in the Attach method */
  private String uri;

  /** The set of Task URIs which we definitely own. */
  private Set<String> owned_uris;

  /** When first created we claim we own everything, to avoid races
   *  where someone calls 'ping' before we have received the full list
   *  of owned tasks. */
  private boolean own_everything;

  public TaskOwner(DBusConnection conn) throws DBusException {
    this.conn = conn;
    String names[] = conn.getNames();
    /* These two errors should never happen. I probably should make
       them into RuntimeExceptions */
    if (names.length == 0) {
      System.err.println("INTERNAL ERROR: the dbus connection has 0 bus names, should be at least 1");
      System.exit(1);
    }
    String bus_name = names[0];
    if (bus_name.equals("")) {
      System.err.println("INTERNAL ERROR: one of the dbus connection names was the empty string");
      System.exit(1);
    }
    /* Use the bus_name as the scheme, but remove a leading ':' prefix.
       These correspond to anonymous names of the form :[\d+].[\d+].
       Regular names cannot start with digits so we can detect when to
       put the ':' back. */
    String scheme = bus_name;
    if (scheme.charAt(0) == ':') scheme = scheme.substring(1);
    String object_path = prefix + Integer.toString(this.getNextId());
    conn.exportObject(object_path, this);
    this.uri = scheme + "://" + object_path;
    this.owned_uris = new HashSet<String>();
    this.own_everything = true;
    System.out.println("task owner URI = " + this.uri);
  }

  public boolean isRemote(){ return false; }

  /** Return our URI, which should be passed to Resource1.{attach,detach} */
  public String getUri() { return this.uri; }

  /** We should claim to own the given URI */
  public void add(String uri) {
    this.owned_uris.add(uri);
  }

  /** We nolonger own the given URI */
  public void remove(String uri) {
    this.owned_uris.remove(uri);
  }

  /** Before we let someone else create a task on our behalf, we should
   *  claim we own everything, to avoid races where we haven't had time
   *  to call [add] before [ping] arrives.
   */
  public void setOwnEverything(boolean own_everything) {
    this.own_everything = own_everything;
  }

  public List<Boolean> ping(List<String> uris){
    System.out.println("PING");
    List<Boolean> result = new Vector<Boolean>();
    for (String uri: uris){
      result.add(this.own_everything || this.owned_uris.contains(uri));
    }
    return result;
  }
}
