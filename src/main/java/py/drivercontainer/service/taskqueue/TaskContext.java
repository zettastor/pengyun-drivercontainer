
package py.drivercontainer.service.taskqueue;

public interface TaskContext {

  /**
   * Get identifier for this context.
   *
   * @return instance of {@link TaskIdentifier}.
   */
  public TaskIdentifier getIdentifier();

  /**
   * Get task in this context.
   *
   */
  public Runnable getTask();
}
