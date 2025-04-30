package nextflow.cws.k8s.model

import groovy.transform.CompileStatic
import nextflow.k8s.model.PodMountConfig

@CompileStatic
class PodMountConfigWithMode extends PodMountConfig {
    private Integer mode

    PodMountConfigWithMode(String config, String mount, Integer mode = null) {
        super(config, mount)
        this.mode = mode
    }

    Integer getMode() {
        return mode
    }

    void setMode(Integer mode) {
        this.mode = mode
    }

}