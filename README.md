# nf-cws plugin 

This plugin enables Nextflow to communicate with a Common Workflow Scheduler instance and transfer the required information.

### Supported Executors

- k8s

### How to use

To run Nextflow with this plugin, you need version >=`tbd`.
To activate the plugin, add the following to your `nextflow.config`:
```
plugins {
  id 'nf-cws'
}
```

### Configuration

| Attribute | Required | Explanation |
|:---:|:---:|---|
| dns | - | Provide the link to the running CWS instance |
| strategy | - | Which strategy should be used for scheduling; available strategies depend on the CWS instance |
| costFunction | - | Which cost function should be used for scheduling; available strategies depend on the CWS instance |

##### Example: 
```
cws {
    dns = 'http://cws-scheduler/'
    strategy = 'rank_max-fair'
    costFunction = 'MinSize'
}
```

#### K8s Executor

The `k8s` executor allows starting a Common Workflow Scheduler instance on demand. If you want to use this feature, you have to define the following:

```
k8s {
    scheduler {
        name = 'workflow-scheduler'
        serviceAccount = 'nextflowscheduleraccount'
        imagePullPolicy = 'IfNotPresent'
        cpu = '2'
        memory = '1400Mi'
        container = 'commonworkflowscheduler/kubernetesscheduler:v1.0'
        command = null
        port = 8080
        workDir = '/scheduler'
        runAsUser = 0
        autoClose = false
        nodeSelector = null
    }
}
```

| Attribute | Required | Explanation |
|:---:|---|---|
| name | - | The name of the pod created |
| serviceAccount | x | Service account used by the scheduler |
| imagePullPolicy | - | Image pull policy for the created pod ([k8s docs](https://kubernetes.io/docs/concepts/containers/images/#image-pull-policy)) |
| cpu | - | Number of cores to use for the scheduler pod |
| memory | - | Memory to use for the scheduler pod |
| container | - | Container image to use for the scheduler pod |
| command | - | Command to start the CWS in the pod. If you need to overwrite the original ENTRYPOINT |
| port | - | Port where to reach the CWS Rest API |
| workDir | - | Workdir within the pod |
| runAsUser | - | Run the scheduler as a specific user |
| autoClose | - | Stop the pod after the workflow is finished |
| nodeSelector | - | A node selector for the CWS pod |