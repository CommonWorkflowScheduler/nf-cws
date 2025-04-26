package nextflow.cws

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.cws.wow.file.WOWFileSystemProvider
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
        WOWFileSystemProvider.INSTANCE.registerSchedulerClient( this )
    }

    protected String getDNS() {
        return dns ? dns + "/v1" : null
    }

    synchronized void registerScheduler( Map data ) {
        if ( registered ) return
        String url = "${getDNS()}/scheduler/$runName"
        registered = true
        int trials = 0
        while ( trials++ < 50 ) {
            try {
                HttpURLConnection post = URI.create(url).toURL().openConnection() as HttpURLConnection
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
        HttpURLConnection post = URI.create("${getDNS()}/scheduler/$runName").toURL().openConnection() as HttpURLConnection
        post.setRequestMethod( "DELETE" )
        int responseCode = post.getResponseCode()
        log.trace "Delete scheduler code was: ${responseCode}"
    }

    void submitMetrics( Map metrics, int id ){
        HttpURLConnection post = URI.create("${getDNS()}/scheduler/$runName/metrics/task/$id").toURL().openConnection() as HttpURLConnection
        post.setRequestMethod( "POST" )
        String message = JsonOutput.toJson( metrics )
        post.setDoOutput(true)
        post.setRequestProperty("Content-Type", "application/json")
        post.getOutputStream().write(message.getBytes("UTF-8"))
        int responseCode = post.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while submitting metrics" )
        }
    }

    Map registerTask( Map config, int id ){

        HttpURLConnection post = URI.create("${getDNS()}/scheduler/$runName/task/$id").toURL().openConnection() as HttpURLConnection
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
        HttpURLConnection put = URI.create("${getDNS()}/scheduler/$runName/${command}Batch").toURL().openConnection() as HttpURLConnection
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

        HttpURLConnection get = URI.create("${getDNS()}/scheduler/$runName/task/$id").toURL().openConnection() as HttpURLConnection
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
        HttpURLConnection put = URI.create("${getDNS()}/scheduler/$runName/DAG/vertices").toURL().openConnection() as HttpURLConnection
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
        HttpURLConnection put = URI.create("${getDNS()}/scheduler/$runName/DAG/edges").toURL().openConnection() as HttpURLConnection
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

    ///* File location */

    Map getFileLocation( String path ){

        String pathEncoded = URLEncoder.encode(path,'utf-8')
        HttpURLConnection get = URI.create("${getDNS()}/file/$runName?path=$pathEncoded").toURL().openConnection() as HttpURLConnection
        get.setRequestMethod( "GET" )
        get.setDoOutput(true)
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting file location: $path (${get.responseMessage})" )
        }
        Map response = new JsonSlurper().parse(get.getInputStream()) as Map
        return response

    }

    String getDaemonOnNode( String node ){

        HttpURLConnection get = URI.create("${getDNS()}/daemon/$runName/$node").toURL().openConnection() as HttpURLConnection
        get.setRequestMethod( "GET" )
        get.setDoOutput(true)
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting daemon on node: $node" )
        }
        String response = new JsonSlurper().parse(get.getInputStream()) as String
        return response

    }

    void addFileLocation( String path, long size, long timestamp, long locationWrapperID, boolean overwrite, String node = null ){

        String method = overwrite ? 'overwrite' : 'add'

        HttpURLConnection get = URI.create("${getDNS()}/file/$runName/location/${method}${ node ? "/$node" : ''}").toURL().openConnection() as HttpURLConnection
        get.setRequestMethod( "POST" )
        get.setDoOutput(true)
        Map data = [
                path      : path,
                size      : size,
                timestamp : timestamp,
                locationWrapperID : locationWrapperID
        ]
        if ( node ){
            data.node = node
        }
        String message = JsonOutput.toJson( data )
        get.setRequestProperty("Content-Type", "application/json")
        get.getOutputStream().write(message.getBytes("UTF-8"))
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while updating file location: $path: $node (${get.responseMessage})" )
        }

    }
}