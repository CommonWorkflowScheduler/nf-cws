package nextflow.cws.wow.file

import nextflow.cws.wow.filesystem.WOWFileSystem
import spock.lang.Specification

import java.nio.file.Path

class OWFileSystemTest extends Specification {

    def "GetPathMatcher"() {
        given:
        def fileSystem = new WOWFileSystem()

        when:
        def matcher = fileSystem.getPathMatcher("glob:*_data")

        then:
        matcher.matches(Path.of("/localdata/localwork/c4/6274337c27f9979441ab60afdd145d/multiqc_data").getFileName())
    }
}
