package nextflow.cws.k8s

import groovy.util.logging.Slf4j
import nextflow.k8s.client.ClientConfig
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson
import org.yaml.snakeyaml.Yaml
import groovy.json.JsonOutput

import java.nio.file.Path

@Slf4j
class CWSK8sClient extends K8sClient {

    /**
     * Creates a kubernetes client using the configuration setting provided by the specified
     * {@link nextflow.k8s.client.ConfigDiscovery} instance
     *
     * @param config
     */
    CWSK8sClient(ClientConfig config) {
        super(config)
    }

    K8sResponseJson podCreate(String req, namespace = config.namespace) {
        assert req
        final action = "/api/v1/namespaces/$namespace/pods"
        final resp = post(action, req)
        return new K8sResponseJson(resp.text)
    }

    K8sResponseJson podCreate(Map req, Path saveYamlPath=null, namespace = config.namespace) {

        if( saveYamlPath ) try {
            saveYamlPath.text = new Yaml().dump(req).toString()
        }
        catch( Exception e ) {
            log.debug "WARN: unable to save request yaml -- cause: ${e.message ?: e}"
        }

        return podCreate(JsonOutput.toJson(req), namespace)
    }

    String podIP( String podName ){
        assert podName
        final resp = podStatus(podName)
        return (resp?.status as Map)?.podIP
    }

}
