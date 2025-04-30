package nextflow.cws

import groovy.transform.CompileStatic
import nextflow.cws.processor.SchedulerBatch

@CompileStatic
class CWSSchedulerBatch extends SchedulerBatch {

    private SchedulerClient schedulerClient

    CWSSchedulerBatch(int batchSize ) {
        super(batchSize)
    }

    void setSchedulerClient(SchedulerClient schedulerClient ) {
        this.schedulerClient = schedulerClient
    }

    void startBatchImpl() {
        schedulerClient.startBatch()
    }

    void endBatch() {
        schedulerClient.endBatch()
    }

}