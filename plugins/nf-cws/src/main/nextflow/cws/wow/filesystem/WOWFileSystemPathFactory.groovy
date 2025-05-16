package nextflow.cws.wow.filesystem

import groovy.transform.CompileStatic
import nextflow.cws.wow.file.LocalPath
import nextflow.file.FileSystemPathFactory

import java.nio.file.Path

@CompileStatic
class WOWFileSystemPathFactory extends FileSystemPathFactory {

    @Override
    protected Path parseUri(String uri) {
        if ( uri.startsWith("wow://") ) {
            def of = Path.of(uri.substring(5))
            return LocalPath.toLocalPath(of, null, null )
        }
        return null
    }

    @Override
    protected String toUriString(Path path) {
        if ( path instanceof LocalPath ) {
            return path.toUriString()
        }
        return null
    }

    @Override
    protected String getBashLib(Path target) {
        null
    }

    @Override
    protected String getUploadCmd(String source, Path target) {
        throw new UnsupportedOperationException("WOWFileSystemPathFactory does not support getUploadCmd")
    }

}
