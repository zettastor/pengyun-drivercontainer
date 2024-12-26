/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
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