package nextflow.cws.wow.serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import groovy.transform.CompileStatic
import nextflow.cws.wow.file.LocalPath

@CompileStatic
class LocalPathSerializer extends Serializer<LocalPath> {

    private static final Map<String, LocalPath> storage = [:]

    @Override
    void write(Kryo kryo, Output output, LocalPath object) {
        def content = object.getInner().toString()
        synchronized (storage) {
            storage.put(content, object)
        }
        output.writeString( content )
    }

    @Override
    LocalPath read(Kryo kryo, Input input, Class<LocalPath> type) {
        def content = input.readString()
        synchronized (storage) {
            return storage.get(content)
        }
    }

}
