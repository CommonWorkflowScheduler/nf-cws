package nextflow.cws.k8s

import groovy.util.logging.Slf4j
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson
import nextflow.util.MemoryUnit

@Slf4j
class CWSK8sClient extends K8sClient {

    CWSK8sClient( K8sClient k8sClient ) {
        super( k8sClient.config )
    }

    Long getPodMemory(String podName){
        assert podName
        final K8sResponseJson resp = podStatus0(podName)
        def containers = (resp?.spec as Map)?.containers as List<Map>
        if ( containers == null || containers.size() == 0 ) {
            return null
        }
        String memory = containers[0]?.resources?.limits?.memory as String
        if ( memory == null ) {
            return null
        } else {
            memory = memory.strip()
            // Convert to the nextflow representation for example: Gi in Kubernetes == GB in nextflow
            if ( memory.endsWith('i') ) {
                memory = memory.substring(0, memory.length() - 1) + 'B'
            }
            return new MemoryUnit(memory).toBytes()
        }
    }

}
