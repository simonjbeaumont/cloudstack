package org.xenserver;

import org.freedesktop.dbus.DBusSigHandler;
import org.xenserver.Task1;

public class CompletedHandler implements DBusSigHandler<Task1.Completed> {
  private boolean finished;

  public CompletedHandler(){
    this.finished = false;
  }

  public synchronized void handle(Task1.Completed sig){
    System.out.println("handle ");
    this.finished = true;
    this.notifyAll();
  }

  public synchronized void waitForCompletion(){
    while (!this.finished){
      try {
        this.wait();
      } catch (InterruptedException e) { }
    }
  }
}
