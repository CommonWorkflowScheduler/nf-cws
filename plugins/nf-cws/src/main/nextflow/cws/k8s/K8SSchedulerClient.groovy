package nextflow.cws.k8s

import groovy.util.logging.Slf4j
import nextflow.cws.CWSConfig
import nextflow.cws.SchedulerClient
import nextflow.exception.NodeTerminationException
import nextflow.k8s.client.K8sResponseException
import nextflow.k8s.model.PodSecurityContext
import nextflow.k8s.model.PodSpecBuilder
import nextflow.k8s.model.PodVolumeClaim

import java.nio.file.Paths

@Slf4j
class K8SSchedulerClient extends SchedulerClient {

    private final CWSK8sConfig.K8sScheduler schedulerConfig
    private final CWSK8sClient k8sClient
    private final String namespace
    private final Collection<PodVolumeClaim> volumeClaims
    private String ip

    K8SSchedulerClient(
            CWSConfig config,
            CWSK8sConfig.K8sScheduler schedulerConfig,
            String namespace,
            String runName,
            CWSK8sClient k8sClient,
            Collection<PodVolumeClaim> volumeClaims
    ) {
        super( config, runName )
        this.volumeClaims = volumeClaims
        this.k8sClient = k8sClient
        this.schedulerConfig = schedulerConfig
        this.namespace = namespace ?: 'default'
    }

    protected String getDNS(){
        return "http://${ip.replace('.','-')}.${namespace}.pod.cluster.local:${schedulerConfig.getPort()}/v1/"
    }

    @Override
    synchronized void registerScheduler(Map data) {
        startScheduler()
        data.dns = getDNS()
        data.namespace = namespace
        super.registerScheduler(data)
    }

    private void startScheduler(){

        boolean start = false
        Map state

        try{
            //If no pod with the name exists an exceptions is thrown
            state = k8sClient.podState( schedulerConfig.getName() )
            if( state.terminated ) {
                k8sClient.podDelete( schedulerConfig.getName() )
                start = true
                log.info "Scheduler ${schedulerConfig.getName()} is terminated"
            } else if( state.running || state.waiting ) log.trace "Scheduler ${schedulerConfig.getName()} is already running"
            else log.error "Unknown state for ${schedulerConfig.getName()}: ${state.toString()}"

        } catch ( K8sResponseException e ) {
            if ( e.getErrorCode() == 404 ) start = true
            else log.error( "Got unexpected HTTP code ${e.getErrorCode()} while checking scheduler's state", e.message )
        } catch ( NodeTerminationException ignored){
            //NodeTerminationException is thrown if pod is not found
            start = true
            log.info "Scheduler ${schedulerConfig.getName()} can not be found"
        }

        if( start ){
            log.trace "Scheduler ${schedulerConfig.getName()} is not running, let's start"
            final builder = new PodSpecBuilder()
                    .withImageName( schedulerConfig.getContainer() )
                    .withPodName( schedulerConfig.getName() )
                    .withCpus( schedulerConfig.getCPUs() )
                    .withMemory( schedulerConfig.getMemory() )
                    .withImagePullPolicy( schedulerConfig.getImagePullPolicy() )
                    .withServiceAccount( schedulerConfig.getServiceAccount() )
                    .withNamespace( namespace )
                    .withLabel('component', 'scheduler')
                    .withLabel('tier', 'control-plane')
                    .withLabel('app', 'nextflow')
                    .withVolumeClaims( volumeClaims )

            if( schedulerConfig.getNodeSelector() )
                builder.setNodeSelector( schedulerConfig.getNodeSelector() )

            if ( schedulerConfig.getWorkDir() )
                builder.withWorkDir( schedulerConfig.getWorkDir() )

            if( schedulerConfig.getCommand() )
                builder.withCommand( schedulerConfig.getCommand() )

            if( schedulerConfig.runAsUser() != null ){
                builder.securityContext = new PodSecurityContext( schedulerConfig.runAsUser() )
            }

            //This is required to use the PodSpecBuilder as it is
            builder.command = [ "delete this" ]
            Map pod = builder.build()

            List env = [[
                                name: 'SCHEDULER_NAME',
                                value: schedulerConfig.getName()
                        ],[
                                name: 'AUTOCLOSE',
                                value: schedulerConfig.autoClose() as String
                        ]]

            Map container = pod.spec.containers.get(0) as Map
            container.put('env', env)
            container.remove( 'command' )
            (container.resources as Map)?.remove( 'limits' )

            k8sClient.podCreate( pod, Paths.get('.nextflow-scheduler.yaml') )
        }

        //wait for scheduler to get ready
        int i = 0
        do {
            sleep(100)
            state = k8sClient.podState( schedulerConfig.getName() )
            //log state every 2 seconds
            if( ++i % 20 == 0 ) log.trace "Waiting for scheduler to start, current state: ${state.toString()}"
        } while ( state.waiting || state.isEmpty() )

        ip = k8sClient.podIP( schedulerConfig.getName() )
        if( !state.running ) throw new IllegalStateException( "Scheduler pod ${schedulerConfig.getName()} was not started, state: ${state.toString()}" )

    }

}
