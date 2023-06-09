package nextflow.cws.k8s

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import nextflow.k8s.K8sConfig
import nextflow.k8s.model.PodNodeSelector

import java.util.stream.Collectors

class CWSK8sConfig extends K8sConfig {

    private Map<String,Object> target

    CWSK8sConfig(Map<String, Object> config) {
        super(config)
        this.target = config
    }

    K8sScheduler getScheduler(){
        return target.scheduler ? new K8sScheduler( (Map<String,Object>)target.scheduler ) : null
    }

    @CompileStatic
    @PackageScope
    static class K8sScheduler {

        Map<String,Object> target

        private final String[] fields = [
                'name',
                'serviceAccount',
                'cpu',
                'memory',
                'container',
                'command',
                'port',
                'workDir',
                'runAsUser',
                'autoClose',
                'nodeSelector',
                'imagePullPolicy'
        ]

        K8sScheduler(Map<String,Object> scheduler) {
            this.target = scheduler
        }

        String getName() { target.name as String ?: 'workflow-scheduler' }

        String getServiceAccount() { target.serviceAccount as String }

        // If no container is specified pull the latest image
        String getImagePullPolicy() { target.container ? target.imagePullPolicy as String : "Always" }

        Integer getCPUs() { target.cpu as Integer ?: 1 }

        String getMemory() { target.memory as String ?: "1400Mi" }

        String getContainer() { target.container as String ?: 'commonworkflowscheduler/kubernetesscheduler:latest' }

        String getCommand() { target.command as String }

        Integer getPort() { target.port as Integer ?: 8080 }

        String getWorkDir() { target.workDir as String }

        Integer runAsUser() { target.runAsUser as Integer }

        Boolean autoClose() { target.autoClose == null ? true : target.autoClose as Boolean }

        PodNodeSelector getNodeSelector(){
            return target.nodeSelector ? new PodNodeSelector( target.nodeSelector ) : null
        }

        Map<String,Object> getAdditional() {
            return target.entrySet()
                    .stream()
                    .filter{!(it.getKey() in fields) }
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        }

        @Memoized
        static K8sScheduler defaultConfig( K8sConfig k8sConfig ){
            return new K8sScheduler([
                    "serviceAccount" : k8sConfig.getServiceAccount(),
                    "runAsUser" : 0,
                    "autoClose" : true
            ] as Map<String, Object>)
        }

    }

}
