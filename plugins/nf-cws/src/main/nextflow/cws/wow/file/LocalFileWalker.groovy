package nextflow.cws.wow.file

import groovy.util.logging.Slf4j
import nextflow.extension.FilesEx

import java.nio.file.FileVisitOption
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

@Slf4j
class LocalFileWalker {

    static final int VIRTUAL_PATH = 0
    static final int FILE_EXISTS = 1
    static final int REAL_PATH = 2
    static final int SIZE = 3
    static final int FILE_TYPE = 4
    static final int CREATION_DATE = 5
    static final int ACCESS_DATE = 6
    static final int MODIFICATION_DATE = 7

    public static TriFunction createLocalPath

    static Path walkFileTree(Path start,
                             Set<FileVisitOption> options,
                             int maxDepth,
                             FileVisitor<? super Path> visitor,
                             Path workDir
    )
    {

        String parent = null
        boolean skip = false

        File file = new File( start.toString() + File.separatorChar + ".command.outfiles" )
        String line
        file.withReader { reader ->
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(';')

                String path = data[ VIRTUAL_PATH ]

                //If Symlink & not followLinks
                //split path
                //save it, and ignore everything at the path behind that

                if ( parent && path.startsWith(parent) ) {
                    if ( skip ) {
                        log.trace "Skip $path"
                        continue
                    }
                } else {
                    skip = false
                }

                FileAttributes attributes = new FileAttributes( data )
                Path currentPath = Paths.get(path)
                if ( !attributes.local ) {
                    //If task did not run on local machine, create symbolic link
                    if ( !Files.isSymbolicLink( currentPath ) ) {
                        Files.createDirectories( currentPath.getParent() )
                        Files.createSymbolicLink( currentPath, attributes.destination )
                    }
                    Files.walkFileTree( currentPath, options, maxDepth, visitor)
                } else {
                    Path p = createLocalPath.apply( currentPath, attributes, workDir )
                    if ( attributes.isDirectory() ) {
                        def visitDirectory = visitor.preVisitDirectory( p, attributes )
                        if( visitDirectory == FileVisitResult.SKIP_SUBTREE ){
                            skip = true
                            parent = path
                        }
                    } else {
                        visitor.visitFile( p, attributes)
                    }
                }
            }
        }

        return start
    }

    static class FileAttributes implements BasicFileAttributes {

        private final boolean directory
        private final boolean link
        private final long size
        private final String fileType
        private final FileTime creationDate
        private final FileTime accessDate
        private final FileTime modificationDate
        private final Path destination
        private final boolean local

        FileAttributes( String[] data ) {
            if ( data.length != 8 && data[ FILE_EXISTS ] != "0" ) throw new RuntimeException( "Cannot parse row (8 columns required): ${data.join(',')}" )
            boolean fileExists = data[ FILE_EXISTS ] == "1"
            destination = data.length > REAL_PATH && data[ REAL_PATH ] ? data[ REAL_PATH ] as Path : null
            if ( data.length != 8 ) {
                this.link = true
                this.size = 0
                this.fileType = null
                this.creationDate = null
                this.accessDate = null
                this.modificationDate = null
                return
            }
            this.link = data[ REAL_PATH ].isEmpty()
            this.size = data[ SIZE ] as Long
            this.fileType = data[ FILE_TYPE ]
            this.accessDate = DateParser.fileTimeFromString(data[ ACCESS_DATE ])
            this.modificationDate = DateParser.fileTimeFromString(data[ MODIFICATION_DATE ])
            this.creationDate = DateParser.fileTimeFromString(data[ CREATION_DATE ]) ?: this.modificationDate
            if ( fileType.startsWith("non-local ") ) {
                this.local = false
                this.fileType = fileType.substring( 10 )
            } else {
                this.local = true
            }
            this.directory = fileType == 'directory'
            if ( !directory && !fileType.contains( 'file' ) ){
                log.error( "Unknown type: $fileType" )
            }
        }

        @Override
        FileTime lastModifiedTime() {
            return modificationDate
        }

        @Override
        FileTime lastAccessTime() {
            return accessDate
        }

        @Override
        FileTime creationTime() {
            return creationDate
        }

        @Override
        boolean isRegularFile() {
            return !directory
        }

        @Override
        boolean isDirectory() {
            return directory
        }

        @Override
        boolean isSymbolicLink() {
            return link
        }

        @Override
        boolean isOther() {
            return false
        }

        @Override
        long size() {
            return size
        }

        @Override
        Object fileKey() {
            return null
        }

        Path getDestination(){
            destination
        }

    }

    static Path exists( final File outfile, final Path file, final Path workDir, final LinkOption option ){

        String rootDirString
        Scanner sc = new Scanner( outfile )
        if( sc.hasNext() )
            rootDirString = sc.next().split(";")[0]
        else
            return null

        final Path rootDir = rootDirString as Path
        final Path fakePath = WOWFileHelper.fakePath( file, rootDir )

        //TODO ignore link
        final Optional<Path> first = Files.lines( outfile.toPath() )
                .parallel()
                .map {line ->
                    String[] data = line.split(';')
                    Path currentPath = data[ VIRTUAL_PATH ] as Path
                    log.trace "Compare $currentPath and $fakePath match: ${(currentPath == fakePath)}"
                    return ( currentPath == fakePath )
                            ? createLocalPath.apply( currentPath, new FileAttributes( data ), workDir )
                            : null
                }
                .filter{ it != null }
                .findFirst()

        return first.isPresent() ? first.get() : null

    }

    static interface TriFunction {
        Path apply( Path path, FileAttributes attributes, Path workDir );
    }

}