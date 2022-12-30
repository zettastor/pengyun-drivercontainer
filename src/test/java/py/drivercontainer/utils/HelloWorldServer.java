/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.drivercontainer.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Bartling, Pintail Consulting LLC.
 *
 * @since Oct 4, 2008 5:48:59 PM
 */
public class HelloWorldServer {

  private ServerSocket server;
  private Socket client;
  private BufferedReader in;
  private PrintWriter out;
  private String line;
  private int listenerPort;

  public HelloWorldServer(int listenerPort) {
    this.listenerPort = listenerPort;
  }

  public static void main(String[] args) {
    final HelloWorldServer helloWorld = new HelloWorldServer(Integer.parseInt(args[0]));
    helloWorld.startListening();
  }


  /**
   * xx.
   */
  public void startListening() {
    try {
      server = new ServerSocket(this.listenerPort);
      client = server.accept();
      in = new BufferedReader(new InputStreamReader(client.getInputStream()));
      out = new PrintWriter(client.getOutputStream(), true);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    while (true) {
      try {
        line = in.readLine();
        if (line.equalsIgnoreCase("hello")) {
          System.out.println("Hello from the server!");
          out.println("ack");
          out.flush();
        } else if (line.equalsIgnoreCase("stop")) {
          try {
            out.println("ack");
            out.flush();
            System.out.println("Client triggered this server to shutdown.");
            in.close();
            out.close();
            server.close();
            System.exit(0);
          } catch (IOException e) {
            System.out.println("Could not close.");
            System.exit(-1);
          }
        }
      } catch (IOException e) {
        System.out.println("Read failed");
        System.exit(-1);
      }
    }
  }
}