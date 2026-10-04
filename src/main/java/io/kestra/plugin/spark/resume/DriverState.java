package io.kestra.plugin.spark.resume;

/**
 * Driver states reported by the Spark standalone Master ({@code org.apache.spark.deploy.master.DriverState}).
 */
public enum DriverState {
    /** Submitted but not yet scheduled on a worker. */
    SUBMITTED,
    /** Allocated to a worker and running. */
    RUNNING,
    /** Exited cleanly. */
    FINISHED,
    /** About to be relaunched (supervised drivers). */
    RELAUNCHING,
    /** Temporarily unknown, while the Master recovers from a failure. */
    UNKNOWN,
    /** Killed on request. */
    KILLED,
    /** Exited non-zero and was not supervised. */
    FAILED,
    /** Could not be run or restarted, e.g. the application jar is missing. */
    ERROR;

    public boolean isTerminal() {
        return this == FINISHED || this == KILLED || this == FAILED || this == ERROR;
    }

    public boolean isSuccess() {
        return this == FINISHED;
    }
}
