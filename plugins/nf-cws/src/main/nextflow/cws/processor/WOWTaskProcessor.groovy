package nextflow.cws.processor

import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.cws.wow.file.WOWFileHelper
import nextflow.exception.MissingFileException
import nextflow.executor.Executor
import nextflow.file.FilePatternSplitter
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.BodyDef
import nextflow.script.BaseScript
import nextflow.script.ProcessConfig
import nextflow.script.params.FileOutParam

import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.Path

@Slf4j
class WOWTaskProcessor extends TaskProcessor {

    WOWTaskProcessor(String name, Executor executor, Session session, BaseScript script, ProcessConfig config, BodyDef taskBody ) {
        super(name, executor, session, script, config, taskBody)
    }

    @PackageScope
    List<Path> fetchResultFiles( FileOutParam param, String namePattern, Path workDir ) {
        assert namePattern
        assert workDir

        List<Path> files = []
        def opts = visitOptions(param, namePattern)
        // scan to find the file with that name
        try {
            WOWFileHelper.visitFiles(opts, workDir, namePattern) { Path it -> files.add(it) }
        }
        catch( NoSuchFileException e ) {
            throw new MissingFileException("Cannot access directory: '$workDir'", e)
        }
        return files.sort()
    }


    @Override
    protected void collectOutFiles(TaskRun task, FileOutParam param, Path workDir, Map context ) {
        final List<Path> allFiles = []
        // type file parameter can contain a multiple files pattern separating them with a special character
        def entries = param.getFilePatterns(context, task.workDir)
        boolean inputsRemovedFlag = false
        // for each of them collect the produced files
        for( String filePattern : entries ) {
            List<Path> result = null

            def splitter = param.glob ? FilePatternSplitter.glob().parse(filePattern) : null
            if( splitter?.isPattern() ) {
                result = fetchResultFiles(param, filePattern, workDir)
                // filter the inputs
                if( result && !param.includeInputs ) {
                    result = filterByRemovingStagedInputs(task, result, workDir)
                    log.trace "Process ${safeTaskName(task)} > after removing staged inputs: ${result}"
                    inputsRemovedFlag |= (result.size()==0)
                }
            }
            else {
                def path = param.glob ? splitter.strip(filePattern) : filePattern
                def file = workDir.resolve(path)
                def origFile = file
                def outfiles = workDir.resolve( ".command.outfiles" ).toFile()
                def exists
                if( outfiles.exists() ){
                    file = LocalFileWalker.exists( outfiles, file, workDir, param.followLinks ? LinkOption.NOFOLLOW_LINKS : null )
                    exists = file != null
                } else {
                    exists = param.followLinks ? file.exists() : file.exists(LinkOption.NOFOLLOW_LINKS)
                }
                if( exists )
                    result = [file]
                else
                    log.debug "Process `${safeTaskName(task)}` is unable to find [${origFile.class.simpleName}]: `$file` (pattern: `$filePattern`)"
            }

            if( result )
                allFiles.addAll(result)

            else if( !param.optional ) {
                def msg = "Missing output file(s) `$filePattern` expected by process `${safeTaskName(task)}`"
                if( inputsRemovedFlag )
                    msg += " (note: input files are not included in the default matching set)"
                throw new MissingFileException(msg)
            }
        }
        task.setOutput( param, allFiles.size()==1 ? allFiles[0] : allFiles )
    }
}
