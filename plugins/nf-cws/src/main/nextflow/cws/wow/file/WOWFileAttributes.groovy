package nextflow.cws.wow.file

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cws.wow.util.DateParser

import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

@Slf4j
@CompileStatic
class WOWFileAttributes implements BasicFileAttributes {

    /**
     * Helper to parse the file attributes returned by the getStatsAndResolveSymlinks.c
     */
    static private final int FILE_EXISTS = 1
    static private final int REAL_PATH = 2
    static private final int SIZE = 3
    static private final int FILE_TYPE = 4
    static private final int CREATION_DATE = 5
    static private final int ACCESS_DATE = 6
    static private final int MODIFICATION_DATE = 7

    private final boolean directory

    private final boolean link

    private final long size

    private final String fileType

    private final FileTime creationDate

    private final FileTime accessDate

    private final FileTime modificationDate

    private final Path destination

    private final boolean local

    WOWFileAttributes( String[] data ) {
        boolean fileExists = data[ FILE_EXISTS ] == "1"
        if ( fileExists && data.length != 8 ) throw new RuntimeException( "Cannot parse row (8 columns required): ${data.join(',')}" )
        destination = data.length > REAL_PATH && data[ REAL_PATH ] ? data[ REAL_PATH ] as Path : null
        if ( data.length != 8 ) {
            this.directory = false
            this.link = true
            this.size = 0
            this.fileType = null
            this.creationDate = null
            this.accessDate = null
            this.modificationDate = null
            this.local = false
            return
        }
        this.link = !data[ REAL_PATH ].isEmpty()
        this.size = data[ SIZE ] as Long
        String fileType = data[ FILE_TYPE ]
        this.accessDate = DateParser.fileTimeFromString(data[ ACCESS_DATE ])
        this.modificationDate = DateParser.fileTimeFromString(data[ MODIFICATION_DATE ])
        this.creationDate = DateParser.fileTimeFromString(data[ CREATION_DATE ]) ?: this.modificationDate
        if ( fileType.startsWith("non-local ") ) {
            this.local = false
            this.fileType = fileType.substring( 10 )
        } else {
            this.local = true
            this.fileType = fileType
        }
        this.directory = fileType == 'directory'
        if ( !directory && !fileType.contains( 'file' ) ){
            log.error( "Unknown type: $fileType" )
        }
    }

    WOWFileAttributes(Path path ) {
        if (path.isDirectory()) {
            directory = true
            link = false
            size = 4096
            fileType = 'directory'
            creationDate = FileTime.fromMillis(0)
            accessDate = FileTime.fromMillis(0)
            modificationDate = FileTime.fromMillis(0)
            destination = path
            local = false
        } else {
            directory = false
            link = path.isLink()
            size = path.size()
            fileType = link ? 'symbolic link' : 'regular file'
            creationDate = null
            accessDate = null
            modificationDate = FileTime.fromMillis(path.lastModified())
            destination = link ? path.toRealPath() : path
            local = !link
        }
    }

    @Override
    FileTime lastModifiedTime() {
        modificationDate
    }

    @Override
    FileTime lastAccessTime() {
        accessDate
    }

    @Override
    FileTime creationTime() {
        creationDate
    }

    @Override
    boolean isRegularFile() {
        !directory
    }

    @Override
    boolean isDirectory() {
        directory
    }

    @Override
    boolean isSymbolicLink() {
        link
    }

    @Override
    boolean isOther() {
        false
    }

    @Override
    long size() {
        size
    }

    @Override
    Object fileKey() {
        null
    }

    Path getDestination(){
        destination
    }

    boolean  isLocal() {
        local
    }

}