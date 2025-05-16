package nextflow.cws.wow.file

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cws.wow.filesystem.WOWFileSystem

import java.nio.file.*

@Slf4j
@CompileStatic
class LocalPath implements Path, Serializable {

    protected final Path path

    private transient final WOWFileAttributes attributes

    boolean createdSymlinks = false

    protected Path workDir

    protected LocalPath(Path path, WOWFileAttributes attributes, Path workDir ) {
        this.path = path
        this.attributes = attributes
        this.workDir = workDir
    }

    Path getInner() {
        path
    }

    LocalPath toLocalPath( Path path, WOWFileAttributes attributes = null ){
        toLocalPath( path, attributes, workDir )
    }

    static LocalPath toLocalPath( Path path, WOWFileAttributes attributes, Path workDir ){
        ( path instanceof LocalPath ) ? path as LocalPath : new LocalPath( path, attributes, workDir )
    }

    <T> T asType( Class<T> c ) {
        if ( c.isAssignableFrom( getClass() ) ) return (T) this
        if ( c.isAssignableFrom( LocalPath.class ) ) return (T) toFile()
        if ( c == String.class ) return (T) toString()
        log.debug("Invoke method asType $c on ${this.class}")
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
        toLocalPath( path.normalize(), attributes )
    }

    @Override
    Path resolve(Path other) {
        toLocalPath( path.resolve( other ), new WOWFileAttributes( other ) )
    }

    @Override
    Path resolve(String other) {
        resolve( Path.of( other ) )
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
            return toLocalPath( path.relativize( localPath.path), (WOWFileAttributes) localPath.attributes, localPath.workDir )
        }
        path.relativize( other )
    }

    @Override
    URI toUri() {
        getFileSystem().provider().getScheme() + "://" + path.toAbsolutePath() as URI
    }

    String toUriString() {
        getFileSystem().provider().getScheme() + ":/" + path.toAbsolutePath()
    }

    Path toAbsolutePath(){
        toLocalPath( path.toAbsolutePath(), attributes )
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

    WOWFileAttributes getAttributes(){
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
        path.hashCode() * 2 + 1
    }

}