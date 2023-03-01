package nextflow.cws

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

@CompileStatic
class CWSPlugin extends BasePlugin {

    CWSPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

}
