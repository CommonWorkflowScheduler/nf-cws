package nextflow.cws.wow.filesystem

import groovy.transform.CompileStatic
import nextflow.cws.SchedulerClient
import nextflow.cws.wow.file.LocalPath

@CompileStatic
class WOWOutputStream extends OutputStream {

    private OutputStream inner

    private SchedulerClient client

    private LocalPath path

    WOWOutputStream(OutputStream inner, SchedulerClient client, LocalPath path) {
        super()
        this.inner = inner
        this.client = client
        this.path = path
    }

    @Override
    void write(int b) throws IOException {
        inner.write(b)
    }

    @Override
    void close() throws IOException {
        inner.close()
        Map location = WOWFileSystemProvider.INSTANCE.getLocation( path )
        File file = path.getInner().toFile()
        client.addFileLocation(path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true)
    }

}
