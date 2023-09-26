package nextflow.cws

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.dag.DAG

@Slf4j
class SchedulerClient {

    private final CWSConfig config
    private final String runName
    private boolean registered = false
    private boolean closed = false
    private int tasksInBatch = 0
    protected String dns

    SchedulerClient( CWSConfig config, String runName ) {
        this.config = config
        this.runName = runName
        this.dns = config.dns?.endsWith('/') ? config.dns[0..-2] : config.dns
        CWSSession.INSTANCE.addSchedulerClient( this )
    }

    protected String getDNS() {
        return dns ? dns + "/v1/" : null
    }

    synchronized void registerScheduler( Map data ) {
        if ( registered ) return
        String url = "${getDNS()}/scheduler/$runName"
        registered = true
        int trials = 0
        while ( trials++ < 50 ) {
            try {
                HttpURLConnection post = new URL(url).openConnection() as HttpURLConnection
                post.setRequestMethod( "POST" )
                post.setDoOutput(true)
                post.setRequestProperty("Content-Type", "application/json")
                data.strategy = config.getStrategy()
                String message = JsonOutput.toJson( data )
                post.getOutputStream().write(message.getBytes("UTF-8"))
                int responseCode = post.getResponseCode()
                if( responseCode != 200 ){
                    throw new IllegalStateException( "Got code: ${responseCode} from k8s scheduler while registering" )
                }
                return
            } catch ( UnknownHostException ignored ) {
                throw new IllegalArgumentException("The scheduler was not found under '$url', is the url correct and the scheduler running?")
            } catch ( ConnectException ignored ) {
                Thread.sleep( 3000 )
            }catch (IOException e) {
                throw new IllegalStateException("Cannot register scheduler under $url, got ${e.class.toString()}: ${e.getMessage()}", e)
            }
        }
        throw new IllegalStateException("Cannot connect to scheduler under $url" )
    }

    synchronized void closeScheduler(){
        if ( closed ) return
        closed = true
        HttpURLConnection post = new URL("${getDNS()}/scheduler/$runName").openConnection() as HttpURLConnection
        post.setRequestMethod( "DELETE" )
        int responseCode = post.getResponseCode()
        log.trace "Delete scheduler code was: ${responseCode}"
    }

    Map registerTask( Map config, int id ){

        HttpURLConnection post = new URL("${getDNS()}/scheduler/$runName/task/$id").openConnection() as HttpURLConnection
        post.setRequestMethod( "POST" )
        String message = JsonOutput.toJson( config )
        post.setDoOutput(true)
        post.setRequestProperty("Content-Type", "application/json")
        post.getOutputStream().write(message.getBytes("UTF-8"))
        int responseCode = post.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while registering task: ${config.name}" )
        }
        tasksInBatch++
        Map response = new JsonSlurper().parse(post.getInputStream()) as Map
        return response

    }

    private void batch( String command ){
        HttpURLConnection put = new URL("${getDNS()}/scheduler/$runName/${command}Batch").openConnection() as HttpURLConnection
        put.setRequestMethod( "PUT" )
        if ( command == 'end' ){
            put.setDoOutput(true)
            put.setRequestProperty("Content-Type", "application/json")
            put.getOutputStream().write("$tasksInBatch".getBytes("UTF-8"))
        }
        int responseCode = put.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while ${command}ing batch" )
        }
    }

    void startBatch(){
        tasksInBatch = 0
        if ( !closed ) batch('start')
    }

    void endBatch(){
        if ( !closed ) batch('end')
    }

    Map getTaskState( int id ){

        HttpURLConnection get = new URL("${getDNS()}/scheduler/$runName/task/$id").openConnection() as HttpURLConnection
        get.setRequestMethod( "GET" )
        get.setDoOutput(true)
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting task state: $id" )
        }
        Map response = new JsonSlurper().parse(get.getInputStream()) as Map
        return response

    }


    ///* DAG */

    void submitVertices( List<DAG.Vertex> vertices ){
        List<Map< String, Object >> verticesToSubmit = vertices.collect {
            [
                    label : it.label,
                    type : it.type.toString(),
                    uid : it.getId()
            ] as Map<String, Object>
        }
        HttpURLConnection put = new URL("${getDNS()}/scheduler/$runName/DAG/vertices").openConnection() as HttpURLConnection
        put.setRequestMethod( "POST" )
        String message = JsonOutput.toJson( verticesToSubmit )
        put.setDoOutput(true)
        put.setRequestProperty("Content-Type", "application/json")
        put.getOutputStream().write(message.getBytes("UTF-8"))
        int responseCode = put.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while submitting vertices: ${vertices}" )
        }
    }

    void submitEdges( List<DAG.Edge> edges ){
        List<Map< String, Object >> edgesToSubmit = edges.collect {
            [
                uid : it.getId(),
                label : it.getLabel(),
                from : it.getFrom()?.getId(),
                to : it.getTo()?.getId()
            ] as Map<String, Object>
        }
        HttpURLConnection put = new URL("${getDNS()}/scheduler/$runName/DAG/edges").openConnection() as HttpURLConnection
        put.setRequestMethod( "POST" )
        String message = JsonOutput.toJson( edgesToSubmit )
        put.setDoOutput(true)
        put.setRequestProperty("Content-Type", "application/json")
        put.getOutputStream().write(message.getBytes("UTF-8"))
        int responseCode = put.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while submitting edges: ${edges}" )
        }
    }

}