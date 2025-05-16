package nextflow.cws.wow.filesystem

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic

import java.nio.file.*
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

@CompileStatic
class WOWFileSystem extends FileSystem {

    static final WOWFileSystem INSTANCE = new WOWFileSystem()

    @Override
    FileSystemProvider provider() {
        WOWFileSystemProvider.INSTANCE
    }

    @Override
    void close() throws IOException {
    }

    @Override
    boolean isOpen() {
        true
    }

    @Override
    boolean isReadOnly() {
        false
    }

    @Override
    String getSeparator() {
        "/"
    }

    @Override
    Iterable<Path> getRootDirectories() {
        throw new UnsupportedOperationException("Root directories not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException("File stores not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        new HashSet<>()
    }

    @Override
    Path getPath(String s, String... strings) {
        throw new UnsupportedOperationException("Path get not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    @CompileDynamic
    PathMatcher getPathMatcher(String s) {
        new PathMatcher() {
            private final def matcher = FileSystems.getDefault().getPathMatcher( s )
            @Override
            boolean matches(Path path) {
                // Make this a Unix Path and use the default matcher
                return matcher.matches( Path.of(path.toString()) )
            }
        }
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("User principal lookup service not supported by ${provider().getScheme().toUpperCase()} file system")
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Watch service not supported by ${provider().getScheme().toUpperCase()} file system")
    }

}
