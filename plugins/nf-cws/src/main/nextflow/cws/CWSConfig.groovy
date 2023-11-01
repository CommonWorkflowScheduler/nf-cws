package nextflow.cws

import groovy.transform.CompileStatic

@CompileStatic
class CWSConfig {

    private final Map<String,Object> target

    CWSConfig(Map<String,Object> scheduler) {
        this.target = scheduler ?: [:]
    }

    String getDns() { target.dns as String }

    String getStrategy() { target.strategy as String ?: 'FIFO' }

    String getCostFunction() { target.costFunction as String }

    String getMemoryPredictor() { target.memoryPredictor as String }

    int getBatchSize() {
        String s = target.batchSize as String
        //Default: 1 -> No batching
        return s ? Integer.valueOf(s) : 1
    }

}
