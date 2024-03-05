package nextflow.cws.k8s

import nextflow.k8s.client.K8sClient

class CWSK8sClient extends K8sClient {

    CWSK8sClient( K8sClient k8sClient ) {
        super( k8sClient.config )
    }

}
