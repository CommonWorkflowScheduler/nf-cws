package nextflow.cws

import groovy.transform.CompileStatic
import nextflow.util.MemoryUnit

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

    MemoryUnit getMaxMemory() {
        String s = target.maxMemory as String
        return s ? new MemoryUnit(s) : null
    }

    MemoryUnit getMinMemory() {
        String s = target.minMemory as String
        return s ? new MemoryUnit(s) : null
    }

    Integer getMaxCopyTasksPerNode() { target.maxCopyTasksPerNode as Integer }

    Integer getMaxWaitingCopyTasksPerNode() { target.maxWaitingCopyTasksPerNode as Integer }

    int getBatchSize() {
        String s = target.batchSize as String
        //Default: 1 -> No batching
        return s ? Integer.valueOf(s) : 1
    }

}
