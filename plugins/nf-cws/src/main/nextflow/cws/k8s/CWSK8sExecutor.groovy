package nextflow.cws.k8s

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.cws.CWSConfig
import nextflow.cws.CWSSchedulerBatch
import nextflow.cws.SchedulerClient
import nextflow.cws.processor.CWSTaskPollingMonitor
import nextflow.k8s.K8sConfig
import nextflow.k8s.K8sExecutor
import nextflow.k8s.model.PodOptions
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

@Slf4j
@CompileStatic
@ServiceName('k8s')
class CWSK8sExecutor extends K8sExecutor implements ExtensionPoint {

    @PackageScope SchedulerClient schedulerClient
    @PackageScope CWSSchedulerBatch schedulerBatch

    @Override
    @Memoized
    protected K8sConfig getK8sConfig() {
        return new CWSK8sConfig( (Map<String,Object>)session.config.k8s )
    }

    /**
     * @return A {@link nextflow.processor.TaskMonitor} associated to this executor type
     */
    @Override
    protected TaskMonitor createTaskMonitor() {
        CWSConfig cwsConfig = new CWSConfig(session.config.navigate('cws') as Map)
        if ( cwsConfig.getBatchSize() > 1 ) {
            this.schedulerBatch = new CWSSchedulerBatch( cwsConfig.getBatchSize() )
        }
        return CWSTaskPollingMonitor.create( session, name, 100, Duration.of('5 sec'), this.schedulerBatch )
    }

    /**
     * Creates a {@link nextflow.processor.TaskHandler} for the given {@link nextflow.processor.TaskRun} instance
     *
     * @param task A {@link nextflow.processor.TaskRun} instance representing a process task to be executed
     * @return A {@link nextflow.k8s.K8sTaskHandler} instance modeling the execution in the K8s cluster
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir
        log.trace "[K8s] launching process > ${task.name} -- work folder: ${task.workDirStr}"
        return new CWSK8sTaskHandler( task, this )
    }

    @Override
    protected void register() {
        super.register()

        CWSK8sConfig.K8sScheduler cwsK8sConfig = (k8sConfig as CWSK8sConfig).getScheduler()
        CWSConfig cwsConfig = new CWSConfig(session.config.navigate('cws') as Map)
        Map data

        if ( !cwsK8sConfig && !cwsConfig.dns ) {
            //Use default configuration
            cwsK8sConfig = CWSK8sConfig.K8sScheduler.defaultConfig( k8sConfig )
        }

        if( cwsK8sConfig ) {
            schedulerClient = new K8sSchedulerClient(
                    cwsConfig,
                    cwsK8sConfig,
                    k8sConfig,
                    k8sConfig.getNamespace(),
                    session.runName,
                    client,
                    k8sConfig.getPodOptions().getVolumeClaims()
            )
            final PodOptions podOptions = k8sConfig.getPodOptions()
            Boolean traceEnabled = session.config.navigate('trace.enabled') as Boolean
            data = [
                    volumeClaims : podOptions.volumeClaims,
                    traceEnabled : traceEnabled,
                    costFunction : cwsConfig.getCostFunction(),
                    memoryPredictor : cwsConfig.getMemoryPredictor(),
                    additional   : cwsK8sConfig.getAdditional()
            ]
        } else {
            data = [
                    dns : cwsConfig.dns,
                    namespace : k8sConfig.getNamespace(),
                    costFunction : cwsConfig.getCostFunction(),
            ]
            schedulerClient = new SchedulerClient( cwsConfig, session.runName )
        }
        this.schedulerBatch?.setSchedulerClient( schedulerClient )
        schedulerClient.registerScheduler( data )
    }

    @Override
    void shutdown() {
        final CWSK8sConfig.K8sScheduler schedulerConfig = (k8sConfig as CWSK8sConfig).getScheduler()
        if( schedulerConfig ) {
            try{
                schedulerClient.closeScheduler()
            } catch (Exception e){
                log.error( "Error while closing scheduler", e)
            }
        }
    }

}
