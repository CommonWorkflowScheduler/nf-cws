package nextflow.cws.wow.util

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cws.wow.file.LocalPath
import nextflow.cws.wow.file.OfflineLocalPath
import nextflow.cws.wow.file.WOWFileAttributes
import nextflow.cws.wow.file.WorkdirHelper

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@Slf4j
@CompileStatic
class LocalFileWalker {

    static private final int VIRTUAL_PATH = 0

    static WorkdirHelper createWorkdirHelper(Path start){
        Map<Path, LocalPath> files = new HashMap<>()
        Path rootPath = null
        File file = new File( start.toString() + File.separatorChar + ".command.outfiles" )
        if ( !file.exists() ) {
            log.warn( "File ${file} does not exist" )
            return null
        }
        String line
        WorkdirHelper workdirHelper
        file.withReader { reader ->
            line = reader.readLine()
            rootPath = Paths.get(line.split(';')[ VIRTUAL_PATH ])
            workdirHelper = new WorkdirHelper( rootPath, files )
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(';')
                String path = data[ VIRTUAL_PATH ]
                WOWFileAttributes attributes = new WOWFileAttributes( data )
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

}