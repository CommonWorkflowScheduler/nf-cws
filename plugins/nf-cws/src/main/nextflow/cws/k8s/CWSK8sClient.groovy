package nextflow.cws.k8s

import groovy.util.logging.Slf4j
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson

@Slf4j
class CWSK8sClient extends K8sClient {

    CWSK8sClient( K8sClient k8sClient ) {
        super( k8sClient.config )
    }

    String getPodMemory(String podName){
        assert podName
        final K8sResponseJson resp = podStatus0(podName)
        def containers = (resp?.spec as Map)?.containers as List<Map>
        if ( containers == null || containers.size() == 0 ) {
            return null
        }
        containers[0]?.resources?.limits?.memory as String
    }

}
