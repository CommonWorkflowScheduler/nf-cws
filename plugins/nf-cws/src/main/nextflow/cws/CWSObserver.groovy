package nextflow.cws

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.dag.DAG
import nextflow.trace.TraceObserver

@Slf4j
@CompileStatic
class CWSObserver implements TraceObserver {

    private DAG dag

    @Override
    void onFlowCreate(Session session) {
        dag = session.dag
    }

    @Override
    void onFlowBegin() {
        dag.normalize()
        CWSSession.INSTANCE.getSchedulerClients().each { schedulerClient ->
            schedulerClient.submitVertices( dag.vertices )
            schedulerClient.submitEdges( dag.edges )
        }
    }

}
