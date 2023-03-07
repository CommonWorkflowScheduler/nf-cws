package nextflow.cws.k8s

import groovy.util.logging.Slf4j
import nextflow.k8s.client.ClientConfig
import nextflow.k8s.client.K8sClient

@Slf4j
class CWSK8sClient extends K8sClient {

    /**
     * Creates a kubernetes client using the configuration setting provided by the specified
     * {@link nextflow.k8s.client.ConfigDiscovery} instance
     *
     * @param config
     */
    CWSK8sClient( ClientConfig config ) {
        super(config)
    }

    String podIP( String podName ){
        assert podName
        final resp = podStatus(podName)
        return (resp?.status as Map)?.podIP
    }

}
