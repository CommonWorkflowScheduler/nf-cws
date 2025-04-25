package nextflow.cws.wow.file

import groovy.util.logging.Slf4j
import nextflow.extension.FilesEx

import java.nio.file.Path

/** This special path is used to represent the workdir path in the local file system.
 * This is required to calculate the correct relative path to the workdir.
 */
@Slf4j
class WorkdirPath extends OfflineLocalPath {

    WorkdirPath(Path path, LocalFileWalker.FileAttributes attributes, Path workDir, WorkdirHelper workdirHelper) {
        super(path, attributes, workDir, workdirHelper)
    }

    @Override
    Path relativize(Path other) {
        if ( other instanceof LocalPath ) {
            def otherPath = ((LocalPath) other).path
            if ( this.path == otherPath ) {
                return Path.of("")
            }
            return workdirHelper.relativeToWorkdir( otherPath )
        }
        return this.path.relativize( other )
    }

    /**
     * This method considers that the current path != the local path.
     * If a file is found in the local paths, it returns the local path.
     * @param other
     * @return
     */
    Path resolve( String other ) {
        def file = this.path.resolve( other )
        workdirHelper.get( file ) ?: file
    }

    /**
     * This method is used to get the relative path of a file in the local file system.
     * @param workdir
     * @param localPath
     * @return
     */
    static Path getRelativePathOnFake(Path workdir, Path localPath ) {
        String n1 = FilesEx.getName(workdir.getParent())
        String n2 = FilesEx.getName(workdir)
        Path p = null
        boolean foundN1 = false
        boolean foundN2 = false
        for (final def part in localPath) {
            if ( part.toString() == n1 && !foundN1 ) {
                foundN1 = true
            } else if ( part.toString() == n2 && foundN1 && !foundN2 ) {
                foundN2 = true
            } else if ( foundN1 && !foundN2 ) {
                return localPath
            } else if ( !foundN1 ) {
                // n1 not found yet, ignore part before
            } else if ( !p ) {
                p = part
            } else {
                p = p.resolve(part)
            }
        }
        return p ?: workdir.relativize( localPath )
    }


}
