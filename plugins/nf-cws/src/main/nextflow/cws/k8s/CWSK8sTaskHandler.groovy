package nextflow.cws.k8s

import groovy.transform.CompileDynamic
import groovy.util.logging.Slf4j
import nextflow.cws.SchedulerClient
import nextflow.extension.GroupKey
import nextflow.file.FileHolder
import nextflow.k8s.K8sTaskHandler
import nextflow.processor.TaskRun
import nextflow.trace.TraceRecord
import org.codehaus.groovy.runtime.GStringImpl

import java.nio.file.NoSuchFileException
import java.nio.file.Path

@Slf4j
class CWSK8sTaskHandler extends K8sTaskHandler {

    static final public String CMD_TRACE_SCHEDULER = '.command.scheduler.trace'

    private SchedulerClient schedulerClient

    private long submitToSchedulerTime = -1

    private long submitToK8sTime = -1

    private final CWSK8sExecutor executor

    CWSK8sTaskHandler( TaskRun task, CWSK8sExecutor executor ) {
        super( task, executor )
        this.schedulerClient = executor.schedulerClient
        this.executor = executor
    }

    protected Map newSubmitRequest0(TaskRun task, String imageName) {
        Map<String, Object> pod = super.newSubmitRequest0(task, imageName)
        if ( (k8sConfig as CWSK8sConfig)?.getScheduler() ){
            (pod.spec as Map).schedulerName = (k8sConfig as CWSK8sConfig).getScheduler().getName() + "-" + getRunName()
        }
        return pod
    }

    @CompileDynamic
    private void extractValue(
            List<Object> booleanInputs,
            List<Object> numberInputs,
            List<Object> stringInputs,
            List<Object> fileInputs,
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
        } else if ( input instanceof String ) {
            stringInputs.add( [ name : key, value : input] )
        } else if ( input instanceof GStringImpl ) {
            stringInputs.add( [ name : key, value : ((GStringImpl) input).toString()] )
        } else {
            log.error ( "input was of class ${input.class}: $input")
            throw new IllegalArgumentException( "Task input was of class and cannot be parsed: ${input.class}: $input" )
        }

    }

    private Map registerTask(){

        final List<Object> booleanInputs = new LinkedList<>()
        final List<Object> numberInputs = new LinkedList<>()
        final List<Object> stringInputs = new LinkedList<>()
        final List<Object> fileInputs = new LinkedList<>()

        for ( entry in task.getInputs() ){
            extractValue( booleanInputs, numberInputs, stringInputs, fileInputs, entry.getKey().name , entry.getValue() )
        }

        Map config = [
                runName : "nf-${task.hash}",
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
        ]
        return schedulerClient.registerTask( config, task.id.intValue() )
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
                case "scheduler_nodes_cost" :
                case "scheduler_init_throughput":
                    traceRecord.put( name, value )
                    break
                case "scheduler_best_cost" :
                    double val = parseDouble( value, file, name )
                    traceRecord.put( name, val )
                    break
                default:
                    long val = parseLong( value, file, name )
                    traceRecord.put( name, val )
            }
        }
    }

    private double parseDouble( String str, Path file , String row )  {
        try {
            return str.toDouble()
        }
        catch( NumberFormatException ignored ) {
            log.debug "[WARN] Not a valid double number `$str` -- offending row: $row in file `$file`"
            return 0
        }
    }

    private long parseLong( String str, Path file , String row )  {
        try {
            return str.toLong()
        }
        catch( NumberFormatException ignored ) {
            log.debug "[WARN] Not a valid long number `$str` -- offending row: $row in file `$file`"
            return 0
        }
    }

    @Override
    TraceRecord getTraceRecord() {
        final TraceRecord traceRecord = super.getTraceRecord()
        traceRecord.put(  "submit_to_scheduler_time", submitToSchedulerTime )
        traceRecord.put(  "submit_to_k8s_time", submitToK8sTime )

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
