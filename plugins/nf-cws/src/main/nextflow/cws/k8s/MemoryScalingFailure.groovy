package nextflow.cws.k8s

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import nextflow.exception.ProcessRetryableException

@InheritConstructors
@CompileStatic
class MemoryScalingFailure extends RuntimeException implements ProcessRetryableException {
}
