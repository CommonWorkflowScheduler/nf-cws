package nextflow.cws.k8s

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cws.SchedulerClient
import nextflow.executor.BashWrapperBuilder
import nextflow.extension.GroupKey
import nextflow.file.FileHolder
import nextflow.k8s.K8sTaskHandler
import nextflow.processor.TaskRun
import nextflow.trace.TraceRecord
import org.codehaus.groovy.runtime.GStringImpl

import java.nio.file.NoSuchFileException
import java.nio.file.Path

@Slf4j
@CompileStatic
class CWSK8sTaskHandler extends K8sTaskHandler {

    static final public String CMD_TRACE_SCHEDULER = '.command.scheduler.trace'

    private SchedulerClient schedulerClient

    private long submitToSchedulerTime = -1

    private long submitToK8sTime = -1

    private final CWSK8sExecutor executor

    private CWSK8sClient client

    private String memoryAdapted = null

    private String syntheticPodName = null

    private long inputSize = -1

    private boolean failedOOM = false

    private final String configMapName

    CWSK8sTaskHandler( TaskRun task, CWSK8sExecutor executor, String configMapName ) {
        super( task, executor )
        this.client = executor.getCWSK8sClient()
        this.schedulerClient = executor.schedulerClient
        this.executor = executor
        this.syntheticPodName = super.getSyntheticPodName(task)
        this.configMapName = configMapName
    }

    @Override
    protected String getSyntheticPodName(TaskRun task) {
        return syntheticPodName
    }

    @Override
    protected Map newSubmitRequest0(TaskRun task, String imageName) {
        Map<String, Object> pod = super.newSubmitRequest0(task, imageName)
        if ( (k8sConfig as CWSK8sConfig)?.getScheduler() ){
            (pod.spec as Map).schedulerName = (k8sConfig as CWSK8sConfig).getScheduler().getName() + "-" + getRunName()
        }

        //Set default mode for configMap
        Map specs = pod.spec as Map
        List<Map> volumes = specs?.volumes as List<Map>
        if ( volumes ) {
            for ( Map vol : volumes ) {
                if ( vol.configMap == null ) continue
                if ( (vol.configMap as Map)?.name == configMapName ) {
                    Map configMap = vol.configMap as Map
                    configMap.defaultMode = 0755
                    break
                }
            }
        }

        return pod
    }

    @CompileDynamic
    private void extractValue(
            List<Map<String,Object>> booleanInputs,
            List<Map<String,Object>> numberInputs,
            List<Map<String,String>> stringInputs,
            List<Map<String,Object>> fileInputs,
            String key,
            Object input
    ){
        if ( input == null ) {
            //Do nothing
        } else if( input instanceof Collection ){
            input.forEach { extractValue(booleanInputs, numberInputs, stringInputs, fileInputs, key, it) }
        } else if( input instanceof Map ) {
            input.entrySet().forEach { extractValue(booleanInputs, numberInputs, stringInputs, fileInputs, key + it.key, it.value) }
        } else if ( input instanceof GroupKey ) {
            extractValue( booleanInputs, numberInputs, stringInputs, fileInputs, key, input.getGroupTarget() )
        } else if( input instanceof FileHolder ){
            fileInputs.add([ name : key, value : [ storePath : input.storePath.toString(), sourceObj : input.sourceObj.toString(), stageName : input.stageName.toString() ]])
        } else if( input instanceof Path ){
            fileInputs.add([ name : key, value : [ storePath : input.toAbsolutePath().toString(), sourceObj : input.toAbsolutePath().toString(), stageName : input.fileName.toString() ]])
        } else if ( input instanceof Boolean ) {
            booleanInputs.add( [ name : key, value : input] )
        } else if ( input instanceof Number ) {
            numberInputs.add( [ name : key, value : input] )
        } else if ( input instanceof Character ) {
            stringInputs.add( [ name : key, value : input as String] )
        } else if ( input instanceof String ) {
            stringInputs.add( [ name : key, value : input] )
        } else if ( input instanceof GStringImpl ) {
            stringInputs.add( [ name : key, value : ((GStringImpl) input).toString()] )
        } else {
            log.error ( "input was of class ${input.class}: $input")
            throw new IllegalArgumentException( "Task input was of class and cannot be parsed: ${input.class}: $input" )
        }
    }

    private static long calculateInputSize(List<Map<String,Object>> fileInputs ){
        return fileInputs
                .parallelStream()
                .mapToLong {
                    final File file = new File(( it.value as Map<String,String>).storePath )
                    return file.directory ? file.directorySize() : file.length()
                }.sum()
    }

    private Map registerTask(){

        final List<Map<String,Object>> booleanInputs = new LinkedList<>()
        final List<Map<String,Object>> numberInputs = new LinkedList<>()
        final List<Map<String,String>> stringInputs = new LinkedList<>()
        final List<Map<String,Object>> fileInputs = new LinkedList<>()

        for ( entry in task.getInputs() ){
            extractValue( booleanInputs, numberInputs, stringInputs, fileInputs, entry.getKey().name , entry.getValue() )
        }


        inputSize = calculateInputSize(fileInputs)
        Map config = [
                runName : syntheticPodName,
                inputs : [
                        booleanInputs : booleanInputs,
                        numberInputs  : numberInputs,
                        stringInputs  : stringInputs,
                        fileInputs    : fileInputs
                ],
                schedulerParams : [:],
                name : task.name,
                task : task.processor.name,
                stageInMode : task.getConfig().stageInMode,
                cpus : task.config.getCpus(),
                memoryInBytes : task.config.getMemory()?.toBytes(),
                workDir : task.getWorkDirStr(),
                repetition : task.failCount,
                inputSize : inputSize
        ]
        return schedulerClient.registerTask( config, task.id.intValue() )
    }

    @Override
    protected BashWrapperBuilder createBashWrapper(TaskRun task) {
        CWSK8sConfig cwsK8sConfig = k8sConfig as CWSK8sConfig
        if ( cwsK8sConfig?.locationAwareScheduling() ) {
            return new WOWK8sWrapperBuilder( task , cwsK8sConfig.getStorage(), executor.getCWSConfig().memoryPredictor as boolean )
        }
        return fusionEnabled()
                ? fusionLauncher()
                : new CWSK8sWrapperBuilder( task, executor.getCWSConfig().memoryPredictor as boolean )
    }

    /**
     * Creates a new K8s pod executing the associated task
     */
    @Override
    @CompileDynamic
    void submit() {
        executor.schedulerBatch?.startSubmit()
        long start = System.currentTimeMillis()
        if ( schedulerClient ) {
            registerTask()
        }
        submitToSchedulerTime = System.currentTimeMillis() - start
        start = System.currentTimeMillis()
        super.submit()
        submitToK8sTime = System.currentTimeMillis() - start
    }

    @Override
    boolean checkIfRunning() {
        try {
            return super.checkIfRunning()
        } catch ( Exception e) {
            log.error("Error checking if task is running", e)
            throw e
        }
    }

    boolean schedulerPostProcessingHasFinished(){
        Map state = schedulerClient.getTaskState(task.id.intValue())
        return (!state.state) ?: ["FINISHED", "FINISHED_WITH_ERROR", "INIT_WITH_ERRORS", "DELETED"].contains( state.state.toString() )
    }

    @Override
    boolean checkIfCompleted() {
        Map state = getState()
        if( !state || !state.terminated || ( (k8sConfig as CWSK8sConfig)?.locationAwareScheduling() && !schedulerPostProcessingHasFinished() ) ) {
            return false
        }
        if( executor.getCWSConfig().memoryPredictor ) {
            memoryAdapted = client.getAdaptedPodMemory( podName )
            if ( memoryAdapted != null ) {
                //only use a special failure logic if the memory was adapted

                def terminated = state.terminated as Map
                if (terminated.exitCode == 128
                        && (terminated.reason as String) == "StartError") {
                    failedOOM = true
                    log.info("The memory was choosen too small for the pod ${podName} to be started. More memory is tried next time.")
                    task.error = new MemoryScalingFailure()
                } else if ((terminated.reason as String) == "OOMKilled") {
                    failedOOM = true
                    log.info("The memory was choosen too small for the pod ${podName} to be executed. More memory is tried next time.")
                    task.error = new MemoryScalingFailure()
                }
            }
        }
        return super.checkIfCompleted()
    }

    private void parseSchedulerTraceFile( Path file, TraceRecord traceRecord ) {

        final text = file.text

        final lines = text.readLines()
        if( !lines )
            return
        if( lines[0] != 'nextflow.scheduler.trace/v1' )
            throw new IllegalStateException("Cannot parse scheduler trace file in version: ${lines[0]}")

        for( int i=0; i<lines.size(); i++ ) {
            final pair = lines[i].tokenize('=')
            final name = pair[0]
            final value = pair[1]
            if( value == null )
                continue
            switch( name ) {
                case 'scheduler_nodes_cost' :
                case 'scheduler_time_delta_phase_three' :
                    traceRecord.put( name, value )
                    break
                case 'scheduler_best_cost' :
                    double val = parseDouble( value, file, name )
                    traceRecord.put( name, val )
                    break
                case 'input_size' :
                    traceRecord.put( name, inputSize )
                    break
                default:
                    long val = parseLong( value, file, name )
                    traceRecord.put( name, val )
            }
        }
    }

    private static double parseDouble(String str, Path file, String row )  {
        try {
            return str.toDouble()
        }
        catch( NumberFormatException ignored ) {
            log.debug "[WARN] Not a valid double number `$str` -- offending row: $row in file `$file`"
            return 0
        }
    }

    private static long parseLong(String str, Path file, String row )  {
        try {
            return str.toLong()
        }
        catch( NumberFormatException ignored ) {
            log.debug "[WARN] Not a valid long number `$str` -- offending row: $row in file `$file`"
            return 0
        }
    }

    protected void deletePodIfSuccessful(TaskRun task) {
        if ( executor.getCWSConfig().memoryPredictor ){
            TraceRecord traceRecord = super.getTraceRecord()
            Map<String,Object> metrics = [
                    ramRequest  : traceRecord.get( "memory" ),
                    peakVmem    : traceRecord.get( "peak_vmem" ),
                    peakRss     : traceRecord.get( "peak_rss" ),
                    realtime   : traceRecord.get( "realtime" ),
            ]
            schedulerClient.submitMetrics( metrics, task.id.intValue() )
        }

        // would not have been cleaned in the super method
        if( !cleanupDisabled() && failedOOM ){
            try {
                client.podDelete(podName)
            } catch( Exception e ) {
                log.warn "Unable to cleanup: $podName -- see the log file for details", e
            }
        }

        super.deletePodIfSuccessful( task )
    }

    @Override
    TraceRecord getTraceRecord() {
        final TraceRecord traceRecord = super.getTraceRecord()
        traceRecord.put( "submit_to_scheduler_time", submitToSchedulerTime )
        traceRecord.put( "submit_to_k8s_time", submitToK8sTime )
        traceRecord.put( "memory_adapted", memoryAdapted )
        traceRecord.put( "input_size", inputSize )

        Path file = task.workDir?.resolve( CMD_TRACE_SCHEDULER )
        try {
            if(file) parseSchedulerTraceFile( file, traceRecord )
        }
        catch( NoSuchFileException ignored ) {
            // ignore it
        }
        catch( IOException e ) {
            log.debug "[WARN] Cannot read scheduler trace file: $file -- Cause: ${e.message}"
        }

        return traceRecord
    }

}
