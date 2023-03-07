package nextflow.cws

/**
 * Central, global instance to manage all generated Executor instances
 */
class CWSSession {

    static final CWSSession INSTANCE = new CWSSession()

    private Set<SchedulerClient> schedulerClients = new HashSet<>()

    synchronized addSchedulerClient( SchedulerClient schedulerClient ) {
        schedulerClients.add(schedulerClient)
    }

    synchronized Set<SchedulerClient> getSchedulerClients() {
        new HashSet(schedulerClients)
    }

    private CWSSession() {}

}
