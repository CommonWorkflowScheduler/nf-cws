package nextflow.cws.wow.file

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cws.k8s.K8sSchedulerClient
import sun.net.ftp.FtpClient

import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

@Slf4j
@CompileStatic
class LocalPath implements Path, Serializable {

    protected final Path path
    private transient final LocalFileWalker.FileAttributes attributes
    private static transient K8sSchedulerClient client = null
    protected Path workDir
    private boolean createdSymlinks = false
    private transient final Object createSymlinkHelper = new Object()

    protected LocalPath(Path path, LocalFileWalker.FileAttributes attributes, Path workDir ) {
        this.path = path
        this.attributes = attributes
        this.workDir = workDir
    }

    private LocalPath(){
        path = null
        this.attributes = null
        this.workDir = null
    }

    Path getInner() {
        return path
    }

    LocalPath toLocalPath( Path path, LocalFileWalker.FileAttributes attributes = null ){
        toLocalPath( path, attributes, workDir )
    }

    static LocalPath toLocalPath( Path path, LocalFileWalker.FileAttributes attributes, Path workDir ){
        ( path instanceof LocalPath ) ? path as LocalPath : new LocalPath( path, attributes, workDir )
    }

    static void setClient( K8sSchedulerClient client ){
        if ( !this.client ) this.client = client
        else throw new IllegalStateException("Client was already set.")
    }

    static FtpClient getConnection(final String node, String daemon ){
        int trial = 0
        while ( true ) {
            try {
                FtpClient ftpClient = FtpClient.create(daemon)
                ftpClient.login("root", "password".toCharArray() )
                ftpClient.enablePassiveMode( true )
                ftpClient.setBinaryType()
                return ftpClient
            } catch ( IOException e ) {
                if ( trial > 5 ) throw e
                log.error("Cannot create FTP client: $daemon on $node", e)
                sleep(Math.pow(2, trial++) as long)
                daemon = client.getDaemonOnNode(node)
            }
        }
    }

    Map getLocation() { getLocation(path.toAbsolutePath().toString()) }

    Map getLocation( String absolutePath ){
        Map response = client.getFileLocation( absolutePath )
        synchronized ( createSymlinkHelper ) {
            if ( !createdSymlinks ) {
                for ( Map link : (response.symlinks as List<Map>)) {
                    Path src = link.src as Path
                    Path dst = link.dst as Path
                    if (Files.exists(src, LinkOption.NOFOLLOW_LINKS)) {
                        try {
                            if (src.isDirectory()) src.deleteDir()
                            else Files.delete(src)
                        } catch ( Exception ignored){
                            log.warn( "Unable to delete " + src )
                        }
                    } else {
                        src.parent.toFile().mkdirs()
                    }
                    try{
                        Files.createSymbolicLink(src, dst)
                    } catch ( Exception ignored){
                        log.warn( "Unable to create symlink: "  + src + " -> " + dst )
                    }
                }
                createdSymlinks = true
            }
        }
        response
    }

    <T> T asType( Class<T> c ) {
        if ( c.isAssignableFrom( getClass() ) ) return (T) this
        if ( c.isAssignableFrom( LocalPath.class ) ) return (T) toFile()
        if ( c == String.class ) return (T) toString()
        log.info("Invoke method asType $c on ${this.class}")
        return super.asType( c )
    }

    String getBaseName() {
        path.getBaseName()
    }

    boolean isDirectory( LinkOption... options ) {
        attributes ? attributes.isDirectory() : 0
    }

    long size() {
        attributes ? attributes.size() : 0
    }

    boolean empty(){
        //TODO empty file?
        this.size() == 0
    }

    boolean asBoolean(){
        true
    }

    @Override
    FileSystem getFileSystem() {
        WOWFileSystem.INSTANCE
    }

    @Override
    boolean isAbsolute() {
        path.isAbsolute()
    }

    @Override
    Path getRoot() {
        path.getRoot()
    }

    @Override
    Path getFileName() {
        path.getFileName()
    }

    @Override
    Path getParent() {
        toLocalPath( path.getParent() )
    }

    @Override
    int getNameCount() {
        path.getNameCount()
    }

    @Override
    Path getName(int index) {
        path.getName( index )
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        toLocalPath( path.subpath( beginIndex, endIndex ) )
    }

    @Override
    boolean startsWith(Path other) {
        path.startsWith( other )
    }

    @Override
    boolean startsWith(String other) {
        path.startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        path.endsWith( other )
    }

    @Override
    boolean endsWith(String other) {
        path.endsWith( other )
    }

    @Override
    Path normalize() {
        toLocalPath( path.normalize() )
    }

    @Override
    Path resolve(Path other) {
        //TODO other attributes
        toLocalPath( path.resolve( other ) )
    }

    @Override
    Path resolve(String other) {
        //TODO other attributes
        toLocalPath( path.resolve( other ) )
    }

    @Override
    Path resolveSibling(Path other) {
        path.resolveSibling( other )
    }

    @Override
    Path resolveSibling(String other) {
        path.resolveSibling( other )
    }

    @Override
    Path relativize(Path other) {
        if ( other instanceof LocalPath ){
            def localPath = (LocalPath) other
            return toLocalPath( path.relativize( localPath.path), (LocalFileWalker.FileAttributes) localPath.attributes, localPath.workDir )
        }
        path.relativize( other )
    }

    @Override
    URI toUri() {
        return getFileSystem().provider().getScheme() + "://" + path.toAbsolutePath() as URI
    }

    String toUriString() {
        return getFileSystem().provider().getScheme() + ":/" + path.toAbsolutePath()
    }

    Path toAbsolutePath(){
        toLocalPath( path.toAbsolutePath() )
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        attributes.destination ? toLocalPath( attributes.destination ) : toLocalPath( path.toRealPath( options ) )
    }

    @Override
    File toFile() {
        new LocalFile( this )
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        path.register( watcher, events, modifiers )
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        path.register( watcher, events )
    }

    @Override
    int compareTo(Path other) {
        if ( other instanceof LocalPath ){
            return path <=> ((LocalPath) other).path
        }
        path <=> other
    }

    @Override
    String toString() {
        path.toString()
    }

    BasicFileAttributes getAttributes(){
        attributes
    }

    @Override
    boolean equals(Object obj) {
        if ( obj instanceof LocalPath ){
            return path == ((LocalPath) obj).path
        }
        if ( obj instanceof Path ){
            return path == obj
        }
        return false
    }

    @Override
    int hashCode() {
        return path.hashCode() * 2 + 1
    }
}