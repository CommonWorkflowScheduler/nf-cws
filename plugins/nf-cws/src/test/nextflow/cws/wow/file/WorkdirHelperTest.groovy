package nextflow.cws.wow.file


import spock.lang.Specification

import java.nio.file.Path

class WorkdirHelperTest extends Specification {

    def "GetDirectoryStream"() {
        String localPath = "/localdata/localwork/be/8292aaebea2ddf9ae8ad4952882dcb"
        String sharedPath = "/input/data/work/be/8292aaebea2ddf9ae8ad4952882dcb"
        def localPath1 = LocalPath.toLocalPath(Path.of(localPath, "file.txt"), null, null)
        def localPath2 = LocalPath.toLocalPath(Path.of(localPath, "a/file2.txt"), null, null)
        def localPath3 = LocalPath.toLocalPath(Path.of(localPath, "b/file3.txt"), null, null)
        def localPath4 = LocalPath.toLocalPath(Path.of(localPath, "b/c/"), null, null)
        def localPath5 = LocalPath.toLocalPath(Path.of(localPath, "b/c/file4.txt"), null, null)
        def localPath6 = LocalPath.toLocalPath(Path.of(localPath, "a/"), null, null)
        def localPath7 = LocalPath.toLocalPath(Path.of(localPath, "b/"), null, null)
        def sharedPath1 = LocalPath.toLocalPath(Path.of(sharedPath, "file.txt"), null, null)
        def sharedPath2 = LocalPath.toLocalPath(Path.of(sharedPath, "a/file2.txt"), null, null)
        def sharedPath3 = LocalPath.toLocalPath(Path.of(sharedPath, "b/file3.txt"), null, null)
        def sharedPath4 = LocalPath.toLocalPath(Path.of(sharedPath, "b/c/"), null, null)
        def sharedPath5 = LocalPath.toLocalPath(Path.of(sharedPath, "b/c/file4.txt"), null, null)
        def sharedPath6 = LocalPath.toLocalPath(Path.of(sharedPath, "a/"), null, null)
        def sharedPath7 = LocalPath.toLocalPath(Path.of(sharedPath, "b/"), null, null)
        Map<Path, LocalPath> files = new HashMap<>()
        files.put( sharedPath1, localPath1 )
        files.put( sharedPath2, localPath2 )
        files.put( sharedPath3, localPath3 )
        files.put( sharedPath4, localPath4 )
        files.put( sharedPath5, localPath5 )
        files.put( sharedPath6, localPath6 )
        files.put( sharedPath7, localPath7 )
        def helper = new WorkdirHelper( Path.of(localPath), files )
        def workDir = Path.of("/input/data/work/be/8292aaebea2ddf9ae8ad4952882dcb")
        def attributes = new  WOWFileAttributes(workDir)
        WorkdirPath path = new WorkdirPath( workDir, attributes, workDir, helper )
        def stream = helper.getDirectoryStream( path )
        def result = stream.collect()

        expect:
        result.size() == 3
        localPath1 in result
        localPath6 in result
        localPath7 in result
    }


}
