package nextflow.cws.wow.file

import java.nio.file.Path

/**
 * We need this class to be able to iterate through a subdirectory of a workdir
 */
class OfflineLocalPath extends LocalPath {

    final protected WorkdirHelper workdirHelper


    OfflineLocalPath( Path path, LocalFileWalker.FileAttributes attributes, Path workDir, WorkdirHelper workdirHelper ) {
        super(path, attributes, workDir)
        this.workdirHelper = workdirHelper
    }

}
