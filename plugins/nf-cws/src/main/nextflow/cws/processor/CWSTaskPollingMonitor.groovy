package nextflow.cws.processor

import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskPollingMonitor
import nextflow.util.Duration

@Slf4j
class CWSTaskPollingMonitor extends TaskPollingMonitor {

    /**
     * Object to batch the task submission to achieve a better scheduling plan
     */
    private final SchedulerBatch schedulerBatch

    /**
     * Create the task polling monitor with the provided named parameters object.
     * <p>
     * Valid parameters are:
     * <li>name: The name of the executor for which the polling monitor is created
     * <li>session: The current {@code Session}
     * <li>capacity: The maximum number of this monitoring queue
     * <li>pollInterval: Determines how often a poll occurs to check for a process termination
     * <li>dumpInterval: Determines how often the executor status is written in the application log file
     *
     * @param params
     */
    protected CWSTaskPollingMonitor(Map params) {
        super(params)
        this.schedulerBatch = params.schedulerBatch as SchedulerBatch
    }

    static TaskPollingMonitor create(Session session, String name, int defQueueSize, Duration defPollInterval, SchedulerBatch schedulerBatch ) {
        assert session
        assert name
        final capacity = session.getQueueSize(name, defQueueSize)
        final pollInterval = session.getPollInterval(name, defPollInterval)
        final dumpInterval = session.getMonitorDumpInterval(name)
        log.debug "Creating task monitor for executor '$name' > capacity: $capacity; pollInterval: $pollInterval; dumpInterval: $dumpInterval; schedulerBatch: $schedulerBatch"
        return new CWSTaskPollingMonitor( name: name, session: session, capacity: capacity, pollInterval: pollInterval, dumpInterval: dumpInterval, schedulerBatch: schedulerBatch )
    }

    /**
     * Loop over the queue of pending tasks and submit all
     * of which satisfy the {@link #canSubmit(nextflow.processor.TaskHandler)}  condition
     *
     * @return The number of tasks submitted for execution
     */
    @Override
    protected int submitPendingTasks() {
        schedulerBatch?.startBatch()
        int pendingTasks = super.submitPendingTasks()
        schedulerBatch?.endBatch()
        return pendingTasks
    }

}
