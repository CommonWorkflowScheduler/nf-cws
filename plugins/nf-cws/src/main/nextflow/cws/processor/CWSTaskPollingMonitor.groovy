package nextflow.cws.processor

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.cws.CWSConfig
import nextflow.cws.wow.file.OfflineLocalPath
import nextflow.cws.wow.file.WOWFileAttributes
import nextflow.cws.wow.file.WorkdirPath
import nextflow.cws.wow.util.LocalFileWalker
import nextflow.processor.PublishDir
import nextflow.processor.TaskHandler
import nextflow.processor.TaskPollingMonitor
import nextflow.util.Duration

@Slf4j
@CompileStatic
class CWSTaskPollingMonitor extends TaskPollingMonitor {

    /**
     * Object to batch the task submission to achieve a better scheduling plan
     */
    private final SchedulerBatch schedulerBatch

    private final CWSConfig cwsConfig

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
        cwsConfig = new CWSConfig(session.config.navigate('cws') as Map)
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

    private static removePublishDirIfSkipEnabled( TaskHandler handler ) {
        if( handler.task.config.get('skipPublishDir') ) {
            handler.task.config.put('publishDir', null)
        }
    }

    private static void checkPublishDirMode(TaskHandler handler ) {
        def publishDirs = handler.task.config.get('publishDir')
        if ( publishDirs && publishDirs instanceof List ) {
            for( Object params : publishDirs ) {
                if( !params ) continue
                if( params instanceof Map ) {
                    def mode = PublishDir.create(params).getMode()
                    // We only support COPY and MOVE, if the user uses a different mode, we set it to COPY
                    if ( !(mode in [ PublishDir.Mode.COPY, PublishDir.Mode.MOVE]) ) {
                        params.mode = PublishDir.Mode.COPY
                    }
                }
            }
        }
    }

    @Override
    protected void finalizeTask(TaskHandler handler) {
        if (!cwsConfig.strategyIsLocationAware()) {
            super.finalizeTask(handler)
            return
        }
        def workDir = handler.task.workDir
        def helper = LocalFileWalker.createWorkdirHelper( workDir )
        if ( helper ) {
            def attributes = new WOWFileAttributes(workDir)
            OfflineLocalPath path = new WorkdirPath( workDir, attributes, workDir, helper )
            handler.task.workDir = path
        }
        removePublishDirIfSkipEnabled(handler)
        checkPublishDirMode(handler)
        super.finalizeTask(handler)
        helper?.validate()
    }
    
}
