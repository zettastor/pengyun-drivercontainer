
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
