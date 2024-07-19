package nextflow.cws.k8s

import groovy.transform.InheritConstructors
import nextflow.exception.ProcessRetryableException

@InheritConstructors
class MemoryScalingFailure extends RuntimeException implements ProcessRetryableException {
}
