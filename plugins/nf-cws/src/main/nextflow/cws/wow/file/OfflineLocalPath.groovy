package nextflow.cws.wow.file

import groovy.transform.CompileStatic

import java.nio.file.Path

/**
 * We need this class to be able to iterate through a subdirectory of a workdir
 */
@CompileStatic
class OfflineLocalPath extends LocalPath {

    final WorkdirHelper workdirHelper

    OfflineLocalPath(Path path, WOWFileAttributes attributes, Path workDir, WorkdirHelper workdirHelper ) {
        super(path, attributes, workDir)
        this.workdirHelper = workdirHelper
    }

}
