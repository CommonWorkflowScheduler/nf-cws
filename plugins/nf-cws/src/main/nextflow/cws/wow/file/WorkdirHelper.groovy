package nextflow.cws.wow.file

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.DirectoryStream
import java.nio.file.Path

@Slf4j
@CompileStatic
class WorkdirHelper {

    private final Map<Path, LocalPath> paths
    private final Path rootPath
    private boolean validated = false

    WorkdirHelper( Path rootPath, Map<Path, LocalPath> paths) {
        this.rootPath = rootPath
        this.paths = paths
    }

    private WorkdirHelper() {
        this(null, null)
    }

    void validate() {
        validated = true
    }

    boolean isValidated() {
        return validated
    }

    LocalPath get( Path path ) {
        if ( validated ) {
            throw new IllegalStateException("WorkdirHelper validated")
        }
        paths.get( path )
    }

    Path relativeToWorkdir( LocalPath path ) {
        new LocalPath( rootPath.relativize( path.path ), (LocalFileWalker.FileAttributes) path.getAttributes(), path.workDir )
    }

    int getNameCount() {
        rootPath.getNameCount()
    }

    DirectoryStream<Path> getDirectoryStream(Path path) {
        if( validated ) {
            throw new IllegalStateException("WorkdirHelper validated")
        }
        // If this is a local path, we need to check if the path is relative to the rootPath
        boolean useLocalPath = path.startsWith(rootPath)
        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                final def cp = path as LocalPath
                def all = paths.entrySet().findAll {
                    Path toCompareAgainst = useLocalPath ? it.value.getInner() : it.key
                    def result = toCompareAgainst.parent == cp.getInner()
                    return result
                }.collect {
                    (Path) it.value
                }
                return all.iterator()
            }
            @Override
            void close() throws IOException {}
        }
    }

    @Override
    String toString() {
        return "WorkdirHelper{" +
                "paths=" + paths +
                ", rootPath=" + rootPath +
                ", validated=" + validated +
                '}'
    }
}
