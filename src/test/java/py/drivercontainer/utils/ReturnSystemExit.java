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

/**
 * xx.
 */

public class ReturnSystemExit {


  /**
   * xx.
   */
  public static void main(String[] args) {
    int returning = Integer.valueOf(args[0]);
    if (returning > 0) {
      System.out.println(1);
    } else {
      System.out.println(0);
    }
  }
}
