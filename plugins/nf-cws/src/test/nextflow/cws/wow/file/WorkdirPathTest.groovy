package nextflow.cws.wow.file


import spock.lang.Specification

import java.nio.file.Path

class WorkdirPathTest extends Specification {

    def "matches"() {
        when:
        def path = Path.of("file.txt").getFileName()

        then:
        path.getFileSystem().getPathMatcher( "glob:*.txt" ).matches( path )
    }

    def "resolve"() {
        when:
        def workDir = Path.of("/input/data/work/be/8292aaebea2ddf9ae8ad4952882dcb")
        def localPath = LocalPath.toLocalPath(Path.of("/localdata/localwork/be/8292aaebea2ddf9ae8ad4952882dcb/file.txt"), null, null)
        Map<Path, LocalPath> files = new HashMap<>()
        files.put(workDir.resolve("file.txt"), localPath)
        def helper = new WorkdirHelper( Path.of("/localdata/localwork/be/8292aaebea2ddf9ae8ad4952882dcb/"), files )
        def attributes = new WOWFileAttributes(workDir)
        WorkdirPath path = new WorkdirPath( workDir, attributes, workDir, helper )

        then:
        def resolve = path.resolve("file.txt")
        resolve == localPath
    }

}
