package nextflow.cws.wow.file

import groovy.util.logging.Slf4j

import java.nio.file.Files
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

    static WorkdirHelper createWorkdirHelper(Path start){
        Map<Path,LocalPath> files = new HashMap<>()
        Path rootPath = null
        File file = new File( start.toString() + File.separatorChar + ".command.outfiles" )
        String line
        WorkdirHelper workdirHelper
        file.withReader { reader ->
            line = reader.readLine()
            rootPath = Paths.get(line.split(';')[ VIRTUAL_PATH ])
            workdirHelper = new WorkdirHelper( rootPath, files )
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(';')
                String path = data[ VIRTUAL_PATH ]
                FileAttributes attributes = new FileAttributes( data )
                Path currentPath = Paths.get(path)
                if ( !attributes.local ) {
                    //If task did not run on local machine, create symbolic link
                    if ( !Files.isSymbolicLink( currentPath ) ) {
                        Files.createDirectories( currentPath.getParent() )
                        Files.createSymbolicLink( currentPath, attributes.destination )
                    }
                } else {
                    def localPath = new OfflineLocalPath(currentPath, attributes, start, workdirHelper)
                    def pathOnSharedFs = start.resolve(rootPath.relativize(currentPath))
                    files.put( pathOnSharedFs, localPath )
                }
            }
            return workdirHelper
        }
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
            this.link = !data[ REAL_PATH ].isEmpty()
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

        FileAttributes( Path path ) {
            directory = true
            link = false
            size = 4096
            fileType = 'directory'
            creationDate = FileTime.fromMillis( 0 )
            accessDate = FileTime.fromMillis( 0 )
            modificationDate = FileTime.fromMillis( 0 )
            destination = path
            local = false
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

}