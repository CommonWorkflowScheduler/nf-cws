package nextflow.cws.wow.file

import groovy.transform.CompileStatic

import java.nio.file.Path

@CompileStatic
class LocalFile extends File {

    private final LocalPath localPath

    LocalFile( LocalPath localPath ){
        super( localPath.toString() )
        this.localPath = localPath
    }

    @Override
    Path toPath() {
        return localPath
    }
}