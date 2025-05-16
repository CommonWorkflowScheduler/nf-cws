package nextflow.cws.wow.filesystem

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.cws.SchedulerClient
import nextflow.cws.wow.file.LocalPath
import nextflow.cws.wow.file.OfflineLocalPath
import nextflow.file.FileSystemTransferAware
import sun.net.ftp.FtpClient

import java.nio.channels.SeekableByteChannel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

@Slf4j
@CompileStatic
class WOWFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    static final WOWFileSystemProvider INSTANCE = new WOWFileSystemProvider()

    protected SchedulerClient schedulerClient = null

    private transient final Object createSymlinkHelper = new Object()

    void registerSchedulerClient(SchedulerClient schedulerClient) throws UnsupportedOperationException {
        if (this.schedulerClient != null) {
            throw new UnsupportedOperationException("WOW file system does not support multiple scheduler clients")
        }
        this.schedulerClient = schedulerClient
    }

    private FtpClient getConnection(final String node, String daemon ){
        int trial = 0
        while ( true ) {
            try {
                FtpClient ftpClient = FtpClient.create(daemon)
                ftpClient.login("root", "password".toCharArray() )
                ftpClient.enablePassiveMode( true )
                ftpClient.setBinaryType()
                return ftpClient
            } catch ( IOException e ) {
                if ( trial > 5 ) {
                    log.error("Cannot create FTP client: $daemon on $node", e)
                    throw e
                }
                sleep(Math.pow(2, trial++) as long)
                daemon = schedulerClient.getDaemonOnNode(node)
            }
        }
    }

    @PackageScope Map getLocation(LocalPath path ){
        String absolutePath = path.toAbsolutePath().toString()
        Map response = schedulerClient.getFileLocation( absolutePath )
        synchronized ( createSymlinkHelper ) {
            if ( !path.createdSymlinks ) {
                for (Map link : (response.symlinks as List<Map>)) {
                    Path src = link.src as Path
                    Path dst = link.dst as Path
                    if (Files.exists(src, LinkOption.NOFOLLOW_LINKS)) {
                        try {
                            if (src.isDirectory()) src.deleteDir()
                            else Files.delete(src)
                        } catch (Exception ignored) {
                            log.warn("Unable to delete " + src)
                        }
                    } else {
                        src.parent.toFile().mkdirs()
                    }
                    try {
                        Files.createSymbolicLink(src, dst)
                    } catch (Exception ignored) {
                        log.warn("Unable to create symlink: " + src + " -> " + dst)
                    }
                }
                path.createdSymlinks = true
            }
        }
        response
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) {
        if (schedulerClient == null) {
            throw new RuntimeException("WOW file system has no registered scheduler client")
        }
        assert path instanceof LocalPath
        Map location = getLocation( path )

        if ( location?.sameAsEngine ) {
            return Files.newInputStream(path.getInner(), options)
        }

        FtpClient ftpClient = getConnection(location.node.toString(), location.daemon.toString())
        InputStream is = ftpClient.getFileStream(location.path.toString())
        return new WOWInputStream(is, schedulerClient, path, ftpClient)
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) {
        if (schedulerClient == null) {
            throw new RuntimeException("WOW file system has no registered scheduler client")
        }
        assert path instanceof LocalPath

        OutputStream os = super.newOutputStream(path.getInner(), options)
        return new WOWOutputStream(os, schedulerClient, path)
    }

    @Override
    String getScheme() {
        "wow"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> map) throws IOException {
        getFileSystem(uri)
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        WOWFileSystem.INSTANCE
    }

    @Override
    Path getPath(URI uri) {
        getFileSystem(uri).getPath(uri.path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("New byte channel not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path path, DirectoryStream.Filter<? super Path> filter) throws IOException {
        if (path instanceof OfflineLocalPath && !((OfflineLocalPath) path).workdirHelper.isValidated()) {
            return ((OfflineLocalPath) path).workdirHelper.getDirectoryStream(path)
        }

        throw new UnsupportedOperationException("Directory stream not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void createDirectory(Path path, FileAttribute<?>... fileAttributes) throws IOException {
        throw new UnsupportedOperationException("Create directory not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void delete(Path path) throws IOException {
        throw new UnsupportedOperationException("Delete not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void copy(Path path, Path path1, CopyOption... copyOptions) throws IOException {
        throw new UnsupportedOperationException("Copy not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void move(Path path, Path path1, CopyOption... copyOptions) throws IOException {
        throw new UnsupportedOperationException("Move not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    boolean isSameFile(Path path1, Path path2) throws IOException {
        path1 == path2
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        path.getFileName().startsWith(".")
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException("File store not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void checkAccess(Path path, AccessMode... accessModes) throws IOException {
        // all access is allowed
    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> aClass, LinkOption... linkOptions) {
        throw new UnsupportedOperationException("File attribute view not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> aClass, LinkOption... linkOptions) throws IOException {
        if (path instanceof LocalPath) {
            return path.getAttributes() as A
        } else {
            return Files.readAttributes(path, BasicFileAttributes.class, linkOptions) as A
        }
    }

    @Override
    Map<String, Object> readAttributes(Path path, String s, LinkOption... linkOptions) throws IOException {
        throw new UnsupportedOperationException("Read attributes not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void setAttribute(Path path, String s, Object o, LinkOption... linkOptions) throws IOException {
        throw new UnsupportedOperationException("Set attribute not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    boolean canUpload(Path source, Path target) {
        false
    }

    @Override
    boolean canDownload(Path source, Path target) {
        true
    }

    @Override
    void download(Path source, Path target, CopyOption... copyOptions) throws IOException {
        try {
            schedulerClient.publish( source, target, "COPY" )
        } catch ( Exception e ) {
            log.error("Error downloading file from ${source} to ${target}", e)
            throw e
        }
    }

    @Override
    void upload(Path source, Path target, CopyOption... copyOptions) throws IOException {
        throw new UnsupportedOperationException("Uploading not supported by ${getScheme().toUpperCase()} file system provider")
    }

}
