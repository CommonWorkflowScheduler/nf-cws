package nextflow.cws

import groovy.transform.CompileStatic
import nextflow.BuildInfo
import nextflow.cws.wow.file.LocalPath
import nextflow.cws.wow.file.OfflineLocalPath
import nextflow.cws.wow.file.WorkdirPath
import nextflow.cws.wow.filesystem.WOWFileSystemProvider
import nextflow.cws.wow.serializer.LocalPathSerializer
import nextflow.file.FileHelper
import nextflow.plugin.BasePlugin
import nextflow.plugin.Plugins
import nextflow.trace.TraceRecord
import nextflow.util.KryoHelper
import org.pf4j.PluginWrapper

@CompileStatic
class CWSPlugin extends BasePlugin {

    CWSPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    private static void registerTraceFields() {
        TraceRecord.FIELDS.putAll( [
                infiles_time:                          'num',
                outfiles_time:                         'num',
                create_bash_wrapper_time:              'num',
                create_request_time:                   'num',
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
                memory_adapted:                        'mem',
                input_size:                            'num',
                scheduler_files_bytes:                 'num',
                scheduler_files_node_bytes:            'num',
                scheduler_files_node_other_task_bytes: 'num',
                scheduler_files:                       'num',
                scheduler_files_node:                  'num',
                scheduler_files_node_other_task:       'num',
                scheduler_depending_task:              'num',
                scheduler_location_count:              'num',
                scheduler_nodes_to_copy_from:          'num',
                scheduler_no_alignment_found:          'num',
                scheduler_time_delta_phase_three:      'str',
                scheduler_copy_tasks:                  'num',
        ] )
    }

    private checkForK8sPlugin() {
        // in 25.03.0-edge, Nextflow's Kubernetes functionality was refactored into a plugin
        boolean isK8sPluginVersion =  getWrapper()
                .getPluginManager()
                .getVersionManager()
                .checkVersionConstraint(BuildInfo.version, ">=25.03.0")

        if ( isK8sPluginVersion ) {
            Plugins.startIfMissing('nf-k8s')
        }
    }

    @Override
    void start() {
        super.start()
        checkForK8sPlugin()
        registerTraceFields()
        KryoHelper.register( LocalPath, LocalPathSerializer )
        KryoHelper.register( OfflineLocalPath, LocalPathSerializer )
        KryoHelper.register( WorkdirPath, LocalPathSerializer )
        FileHelper.getOrInstallProvider(WOWFileSystemProvider)
    }

}
