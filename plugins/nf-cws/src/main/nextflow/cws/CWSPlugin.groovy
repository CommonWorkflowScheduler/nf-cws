package nextflow.cws

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.trace.TraceRecord
import org.pf4j.PluginWrapper

@CompileStatic
class CWSPlugin extends BasePlugin {

    CWSPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    private static void registerTraceFields() {
        TraceRecord.FIELDS.putAll( [
                submit_to_scheduler_time:              'num',
                submit_to_k8s_time:                    'num',
                scheduler_time_in_queue:               'num',
                scheduler_place_in_queue:              'num',
                scheduler_tried_to_schedule:           'num',
                scheduler_time_to_schedule:            'num',
                scheduler_nodes_tried:                 'num',
                scheduler_nodes_cost:                  'str',
                scheduler_could_stop_fetching:         'num',
                scheduler_best_cost:                   'num',
                scheduler_delta_schedule_submitted:    'num',
                scheduler_delta_schedule_alignment:    'num',
                scheduler_batch_id:                    'num',
                scheduler_delta_batch_start_submitted: 'num',
                scheduler_delta_batch_start_received:  'num',
                scheduler_delta_batch_closed_batch_end:'num',
                scheduler_delta_submitted_batch_end:   'num',
        ] )
    }

    @Override
    void start() {
        super.start()
        registerTraceFields()
    }

}
