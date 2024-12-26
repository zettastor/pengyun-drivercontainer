

package py.drivercontainer.service.taskqueue;

public class TaskContextImpl implements TaskContext {

  private final TaskIdentifier identifier;

  private final Runnable task;


  /**
   * xx.
   */
  public TaskContextImpl(TaskIdentifier identifier, Runnable task) {
    super();
    this.identifier = identifier;
    this.task = task;
  }

  @Override
  public TaskIdentifier getIdentifier() {
    return identifier;
  }

  @Override
  public Runnable getTask() {
    return task;
  }

}
