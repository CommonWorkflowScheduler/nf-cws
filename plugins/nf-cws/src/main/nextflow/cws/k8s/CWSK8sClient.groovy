package nextflow.cws.k8s

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.k8s.client.ClientConfig
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseJson
import nextflow.util.MemoryUnit
import org.yaml.snakeyaml.Yaml

import java.nio.file.Path

@Slf4j
@CompileStatic
class CWSK8sClient extends K8sClient {

    CWSK8sClient(K8sClient k8sClient) {
        super(k8sClient.config)
    }

    CWSK8sClient(ClientConfig config) {
        super(config)
    }

    static private void trace(String method, String path, String text) {
        log.trace "[CWS-K8s] API response $method $path \n${prettyPrint(text).indent()}"
    }

    /**
     * Create a pod
     *
     * See
     *  https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#create-55
     *  https://v1-8.docs.kubernetes.io/docs/api-reference/v1.8/#pod-v1-core
     *
     * @param spec
     * @return
     */
    K8sResponseJson podCreate(String req, namespace = config.namespace) {
        assert req
        final action = "/api/v1/namespaces/$namespace/pods"
        final resp = post(action, req)
        trace('POST', action, resp.text)
        return new K8sResponseJson(resp.text)
    }

    K8sResponseJson podCreate(Map req, Path saveYamlPath=null, namespace = config.namespace) {

        if( saveYamlPath ) try {
            saveYamlPath.text = new Yaml().dump(req).toString()
        }
        catch( Exception e ) {
            log.debug "WARN: unable to save request yaml -- cause: ${e.message ?: e}"
        }

        podCreate(JsonOutput.toJson(req), namespace)
    }

    K8sResponseJson daemonSetCreate(Map req, Path saveYamlPath=null) {
        if (saveYamlPath) {
            try {
                saveYamlPath.text = new Yaml().dump(req).toString()
            }
            catch (Exception e) {
                log.debug "WARN: unable to save request yaml -- cause: ${e.message ?: e}"
            }
        }
        daemonSetCreate(JsonOutput.toJson(req))
    }

    K8sResponseJson daemonSetCreate(String req) {
        assert req
        final action = "/apis/apps/v1/namespaces/$config.namespace/daemonsets"
        log.debug "TRYING... $action"
        final resp = post(action, req)
        trace('POST', action, resp.text)
        new K8sResponseJson(resp.text)
    }

    K8sResponseJson daemonSetDelete(String name) {
        assert name
        final action = "/apis/apps/v1/namespaces/$config.namespace/daemonsets/$name"
        final resp = delete(action)
        trace('DELETE', action, resp.text)
        new K8sResponseJson(resp.text)
    }

    K8sResponseJson configCreateBinary(String name, Map data) {

        final spec = [
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: [ name: name, namespace: config.namespace ],
                binaryData: data
        ]

        configCreate0(spec)
    }

    /**
     * Get the memory of a pod that has been adapted by the CWS
     * @param podName The name of the pod
     * @return The memory of the pod in bytes, null if the pod has not been adapted
     */
    Long getAdaptedPodMemory(String podName){
        assert podName
        final K8sResponseJson resp = podStatus0(podName)

        //If this label is not set, the memory was not scaled
        if ( ((resp?.metadata as Map)?.labels as Map)?."commonworkflowscheduler/memoryscaled" != 'true' ) {
            return null
        }

        def containers = (resp?.spec as Map)?.containers as List<Map>
        if ( containers == null || containers.size() == 0 ) {
            return null
        }
        String memory = ((containers[0]?.resources as Map)?.limits as Map)?.memory as String
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
