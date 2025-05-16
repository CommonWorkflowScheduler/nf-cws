package nextflow.cws.k8s

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper
import nextflow.processor.TaskRun
import nextflow.util.Escape

import java.nio.file.Path

/**
 * Implements a BASH wrapper for tasks executed by kubernetes cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@Slf4j
class WOWK8sWrapperBuilder extends CWSK8sWrapperBuilder {

    CWSK8sConfig.Storage storage
    Path localWorkDir

    static String statFileName = "getStatsAndSymlinks"

    WOWK8sWrapperBuilder(TaskRun task, CWSK8sConfig.Storage storage, boolean memoryPredictorEnabled) {
        super(task, memoryPredictorEnabled)
        this.headerScript = "NXF_CHDIR=${Escape.path(task.workDir)}"
        this.storage = storage
        if( storage ){
            switch (storage.getCopyStrategy().toLowerCase()) {
                case 'copy':
                case 'ftp':
                    if ( this.scratch == null || this.scratch == true ){
                        //Reduce amount of local data - only keep necessary outputs
                        this.scratch = (storage.getWorkdir() as Path).resolve( "scratch" ).toString()
                        this.stageOutMode = 'move'
                    }
                    break
                default :
                    throw new IllegalArgumentException("Unsupported copy strategy: ${storage.getCopyStrategy()}")
            }
            if ( !this.targetDir || workDir == targetDir ) {
                this.localWorkDir = FileHelper.getWorkFolder(storage.getWorkdir() as Path, task.getHash())
                this.targetDir = this.localWorkDir
            }
        }
    }

    @Override
    protected boolean shouldUnstageOutputs() {
        assert localWorkDir
        return super.shouldUnstageOutputs()
    }

    private String getStorageLocalWorkDir() {
        String localWorkDir = storage.getWorkdir()
        if ( !localWorkDir.endsWith("/") ){
            localWorkDir += "/"
        }
        localWorkDir
    }

    @Override
    protected Map<String, String> makeBinding() {
        final Map<String,String> binding = super.makeBinding()

        binding.K8sResolveSymlinks = null

        if ( binding.stage_inputs && storage && localWorkDir ) {
            final String cmd = """\
                    # create symlinks
                    if test -f "${workDir.toString()}/.command.symlinks"; then
                        bash "${workDir.toString()}/.command.symlinks" || true
                    fi 
            """.stripIndent()
            binding.stage_inputs = cmd + binding.stage_inputs
        }

        if ( localWorkDir ) {
            binding.unstage_outputs = copyStrategy.getUnstageOutputFilesScript(outputFiles, localWorkDir)
        }

        return binding
    }

    @Override
    protected String getLaunchCommand(String interpreter, String env) {
        assert storage && localWorkDir
        String cmd = ''
        cmd += "local INFILESTIME=\$(\"/etc/nextflow/${statFileName}\" infiles \"${workDir.toString()}/.command.infiles\" \"${getStorageLocalWorkDir()}\" \"\$PWD/\" || true)\n"
        cmd += super.getLaunchCommand(interpreter, env)
        if( isTraceRequired() ){
            cmd += "\nlocal exitCode=\$?"
            cmd += """\necho \"infiles_time=\${INFILESTIME}" >> ${workDir.resolve(TaskRun.CMD_TRACE)}\n"""
            cmd += "return \$exitCode\n"
        }
        return cmd
    }

    @Override
    String getCleanupCmd(String scratch) {
        assert storage && localWorkDir
        String cmd = super.getCleanupCmd( scratch )
        cmd += "mkdir -p \"${localWorkDir.toString()}/\" || true\n"
        cmd += "local OUTFILESTIME=\$(\"/etc/nextflow/${statFileName}\" outfiles \"${workDir.toString()}/.command.outfiles\" \"${getStorageLocalWorkDir()}\" \"${localWorkDir.toString()}/\" || true)\n"
        if ( isTraceRequired() ) {
            cmd += "echo \"outfiles_time=\${OUTFILESTIME}\" >> ${workDir.resolve(TaskRun.CMD_TRACE)}"
        }
        return cmd
    }

}