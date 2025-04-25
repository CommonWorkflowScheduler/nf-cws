package nextflow.cws.k8s.model

import nextflow.k8s.model.PodMountConfig

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