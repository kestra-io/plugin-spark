package io.kestra.plugin.spark.resume;

// Mirrors org.apache.spark.deploy.master.DriverState
public enum DriverState {
    SUBMITTED,
    RUNNING,
    FINISHED,
    // a supervised driver about to be relaunched, so not an outcome yet
    RELAUNCHING,
    // reported while the Master recovers from a failure, so not an outcome yet
    UNKNOWN,
    KILLED,
    FAILED,
    ERROR;

    public boolean isTerminal() {
        return this == FINISHED || this == KILLED || this == FAILED || this == ERROR;
    }

    public boolean isSuccess() {
        return this == FINISHED;
    }
}
