package nextflow.cws.wow.file

import nextflow.cws.SchedulerClient

class WOWInputStream extends InputStream {

    private InputStream inner
    private SchedulerClient client
    private LocalPath path

    private File temporaryFile
    private OutputStream temporaryFileStream
    private boolean transferredTemporaryFile

    WOWInputStream(InputStream inner, SchedulerClient client, LocalPath path) {
        super()
        this.inner = inner
        this.client = client
        this.path = path
        this.temporaryFile = File.createTempFile("local", "buffer")
        this.temporaryFileStream = temporaryFile.newOutputStream()
        this.transferredTemporaryFile = false
    }

    private void checkTemporaryFileTransferal() {
        if (transferredTemporaryFile || inner.available() > 0) {
            return
        }
        temporaryFileStream.flush()
        temporaryFileStream.close()

        File file = path.getInner().toFile()
        temporaryFile.moveTo(file)
        transferredTemporaryFile = true

        Map location = path.getLocation()
        client.addFileLocation(path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, false)
    }

    @Override
    int read() throws IOException {
        int b = inner.read()
        temporaryFileStream.write(b)
        return b
    }

    @Override
    void close() throws IOException {
        inner.close()
        checkTemporaryFileTransferal()
    }
}
