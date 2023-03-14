package nextflow.cws.k8s

import groovy.transform.CompileStatic
import nextflow.k8s.K8sConfig
import nextflow.k8s.model.PodNodeSelector

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
    static class K8sScheduler {

        Map<String,Object> target

        K8sScheduler(Map<String,Object> scheduler) {
            this.target = scheduler
        }

        String getName() { target.name as String ?: 'workflow-scheduler' }

        String getServiceAccount() { target.serviceAccount as String }

        String getImagePullPolicy() { target.imagePullPolicy as String }

        Integer getCPUs() { target.cpu as Integer ?: 1 }

        String getMemory() { target.memory as String ?: "1400Mi" }

        String getContainer() { target.container as String ?: 'commonworkflowscheduler/kubernetesscheduler:v1.0' }

        String getCommand() { target.command as String }

        Integer getPort() { target.port as Integer ?: 8080 }

        String getWorkDir() { target.workDir as String }

        Integer runAsUser() { target.runAsUser as Integer }

        Boolean autoClose() { target.autoClose == null ? true : target.autoClose as Boolean }

        PodNodeSelector getNodeSelector(){
            return target.nodeSelector ? new PodNodeSelector( target.nodeSelector ) : null
        }

    }

}
