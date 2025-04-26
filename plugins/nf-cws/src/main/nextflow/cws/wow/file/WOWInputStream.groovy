package nextflow.cws.wow.file

import nextflow.cws.SchedulerClient
import sun.net.ftp.FtpClient

class WOWInputStream extends InputStream {

    private InputStream inner
    private SchedulerClient schedulerClient
    private LocalPath path
    private boolean fullyRead
    private FtpClient ftpClient

    private File temporaryFile
    private OutputStream temporaryFileStream
    private boolean transferredTemporaryFile

    WOWInputStream(InputStream inner, SchedulerClient schedulerClient, LocalPath path, FtpClient ftpClient) {
        super()
        this.inner = inner
        this.schedulerClient = schedulerClient
        this.path = path
        this.fullyRead = false
        this.ftpClient = ftpClient
        this.temporaryFile = File.createTempFile("local", "buffer")
        this.temporaryFileStream = temporaryFile.newOutputStream()
        this.transferredTemporaryFile = false
    }

    private void checkTemporaryFileTransferal() {
        if (transferredTemporaryFile || !fullyRead) {
            return
        }
        temporaryFileStream.flush()
        temporaryFileStream.close()

        File file = path.getInner().toFile()
        temporaryFile.moveTo(file)
        transferredTemporaryFile = true

        Map location = path.getLocation()
        schedulerClient.addFileLocation(path.toString(), file.size(), file.lastModified(), location.locationWrapperID as long, false)
    }

    @Override
    int available() throws IOException {
        return inner.available()
    }

    @Override
    int read() throws IOException {
        int b = inner.read()
        temporaryFileStream.write(b)
        if (b == -1) {
            fullyRead = true
            checkTemporaryFileTransferal()
        }
        return b
    }

    @Override
    void close() throws IOException {
        fullyRead = inner.read() == -1
        inner.close()
        ftpClient.close()
        checkTemporaryFileTransferal()
    }
}
