package org.apache.mesos.task;

import org.apache.mesos.Protos;

/**
 * Provides utility functions for working with Tasks.
 */
public class TaskUtil {

  public static boolean isTerminated(Protos.TaskStatus taskStatus) {
    Protos.TaskState taskState = taskStatus.getState();
    return taskState.equals(Protos.TaskState.TASK_FINISHED) ||
      taskState.equals(Protos.TaskState.TASK_FAILED) ||
      taskState.equals(Protos.TaskState.TASK_KILLED) ||
      taskState.equals(Protos.TaskState.TASK_LOST) ||
      taskState.equals(Protos.TaskState.TASK_ERROR);
  }
}
