package nextflow.cws.wow.file

import nextflow.cws.SchedulerClient

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
        super.write(b)
    }

    @Override
    void close() throws IOException {
        Map location = path.getLocation()
        File file = path.getInner().toFile()
        client.addFileLocation(path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, true)
    }
}
