package nextflow.cws.wow.file

import groovy.util.logging.Slf4j
import nextflow.cws.SchedulerClient
import nextflow.file.FileSystemTransferAware
import sun.net.ftp.FtpClient

import java.nio.channels.SeekableByteChannel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

@Slf4j
class WOWFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    static final WOWFileSystemProvider INSTANCE = new WOWFileSystemProvider()

    protected SchedulerClient schedulerClient = null // TODO: support multiple clients

    void registerSchedulerClient(SchedulerClient schedulerClient) throws UnsupportedOperationException {
        if (schedulerClient != null) {
            throw new UnsupportedOperationException("WOW file system does not support multiple scheduler clients")
        }
        this.schedulerClient = schedulerClient
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) {
        if (schedulerClient != null) {
            throw new RuntimeException("WOW file system has no registered scheduler client")
        }
        assert path instanceof LocalPath
        Map location = (path as LocalPath).getLocation()

        try (FtpClient ftpClient = path.getConnection(location.node.toString(), location.daemon.toString())) {
            try (InputStream is = ftpClient.getFileStream(location.path.toString())) {
                return new WOWInputStream(is, schedulerClient, path)
            }
        }
    }

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) {
        if (schedulerClient != null) {
            throw new RuntimeException("WOW file system has no registered scheduler client")
        }
        assert path instanceof LocalPath

        OutputStream os = super.newOutputStream(path.getInner(), options)
        return new WOWOutputStream(os, schedulerClient, path)
    }

    @Override
    String getScheme() {
        return "wow"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> map) throws IOException {
        return getFileSystem(uri)
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        return WOWFileSystem.INSTANCE
    }

    @Override
    Path getPath(URI uri) {
        return getFileSystem(uri).getPath(uri.path)
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> set, FileAttribute<?>... fileAttributes) throws IOException {
        log.info("FRIEDRICH: newByteChannel")
        LocalPath localPath = (LocalPath) path
        // TODO: find an alternative to always downloading
        Map downloadResult = localPath.download()
        assert (downloadResult.wasDownloaded || downloadResult.location.sameAsEngine)
        return Files.newByteChannel(localPath.getInner())
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
        throw new UnsupportedOperationException("move not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    boolean isSameFile(Path path1, Path path2) throws IOException {
        return path1 == path2
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        return path.getFileName().startsWith(".")
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException("File store not supported by ${getScheme().toUpperCase()} file system provider")
    }

    @Override
    void checkAccess(Path path, AccessMode... accessModes) throws IOException {
        // TODO: re-check that this may be empty
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
        return false
    }

    @Override
    boolean canDownload(Path source, Path target) {
        return true
    }

    @Override
    void download(Path source, Path target, CopyOption... copyOptions) throws IOException {
        log.warn("Work in progress: Implementation for downloading functionality may not be correct or complete in ${getScheme().toUpperCase()} file system provider")
        // do nothing, as data downloading is handled by the WOW scheduler
    }

    @Override
    void upload(Path source, Path target, CopyOption... copyOptions) throws IOException {
        throw new UnsupportedOperationException("Uploading not supported by ${getScheme().toUpperCase()} file system provider")
    }
}
