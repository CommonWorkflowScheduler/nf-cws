# nf-cws plugin 

This plugin enables Nextflow to communicate with a Common Workflow Scheduler instance and transfer the required information.

### Supported Executors

- k8s

### How to use

To run Nextflow with this plugin, you need version >`23.02.01-edge`.
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

### Tracing
This plugin adds additional fields to the trace report. Therefore, you have to add the required fields to the `trace.fields` field in your Nextflow config (also check the official [documentation](https://www.nextflow.io/docs/latest/tracing.html#trace-report)).
The following fields can be used:
| Name | Description |
| :--- | :--- |
| submit_to_scheduler_time               | Time in ms to register the task at CWS |
| submit_to_k8s_time                     | Time to create and submit pod to k8s |
| scheduler_time_in_queue                | How long was the task in the queue until it got scheduled |
| scheduler_place_in_queue               | At which place was the task in the queue when it got scheduled |
| scheduler_tried_to_schedule            | How often was a scheduling plan calculated until the task was assigned |
| scheduler_time_to_schedule             | How long did it take to calculate the location for this task |
| scheduler_nodes_tried                  | How many nodes have been compared |
| scheduler_nodes_cost                   | Cost value to schedule on the different nodes (only available for some algorithms) |
| scheduler_could_stop_fetching          | How often could the scheduler skip a node |
| scheduler_best_cost                    | Cost on the selected node (only available for some algorithms) |
| scheduler_delta_schedule_submitted     | Time delta between starting to calculate the scheduling plan and submitting the task to the target node |
| scheduler_delta_schedule_alignment     | Time delta between beginning to calculate the scheduling plan and finding the target node |
| scheduler_batch_id                     | The id of the batch the task belongs to |
| scheduler_delta_batch_start_submitted  | Time delta between a batch was started, and the scheduler received this task from the workflow engine |
| scheduler_delta_batch_start_received   | Time delta between a batch was started, and the scheduler received the pod from the k8s API |
| scheduler_delta_batch_closed_batch_end | Time delta between a batch was closed by the workflow engine, and the scheduler received the pod from the k8s API |
| scheduler_delta_submitted_batch_end    | Time delta between a task was submitted, and the batch became schedulable |