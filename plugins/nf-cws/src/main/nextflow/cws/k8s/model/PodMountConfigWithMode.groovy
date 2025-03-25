package nextflow.cws.k8s.model;

import nextflow.k8s.model.PodMountConfig;

public class PodMountConfigWithMode extends PodMountConfig {
    private Integer mode;

    public PodMountConfigWithMode(String config, String mount, Integer mode = null) {
        super(config, mount);
        this.mode = mode;
    }

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

}