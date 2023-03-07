package nextflow.cws.processor

abstract class SchedulerBatch {

    private final int batchSize
    private int currentlySubmitted = 0

    SchedulerBatch( int batchSize ) {
        this.batchSize = batchSize
        assert batchSize > 1
    }

    void startBatch() {
        currentlySubmitted = 0
        startBatchImpl()
    }

    abstract protected void startBatchImpl()

    abstract void endBatch()

    void startSubmit() {
        if ( ++currentlySubmitted > batchSize ) {
            endBatch()
            startBatchImpl()
            currentlySubmitted = 1
        }
    }

}