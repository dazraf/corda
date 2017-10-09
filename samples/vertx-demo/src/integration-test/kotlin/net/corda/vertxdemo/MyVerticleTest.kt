package net.corda.vertxdemo

import io.vertx.core.Future
import io.vertx.core.Future.future
import io.vertx.core.VertxOptions
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import net.corda.core.utilities.getOrThrow
import net.corda.finance.flows.CashIssueFlow
import net.corda.finance.flows.CashPaymentFlow
import net.corda.finance.schemas.CashSchemaV1
import net.corda.node.services.FlowPermissions
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.nodeapi.User
import net.corda.nodeapi.internal.ServiceInfo
import net.corda.testing.*
import net.corda.testing.node.NodeBasedTest
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import rx.Observable
import java.net.URI


@RunWith(VertxUnitRunner::class)
class MyVerticleTest : NodeBasedTest() {
    companion object {
        val BANK_A_URL = URI.create("http://localhost:${40_000}")!!
    }

    private val vertxOptions = VertxOptions().setWarningExceptionTime(10_000)
        .setMaxEventLoopExecuteTime(10_000)
        .setBlockedThreadCheckInterval(10_000)

    @Rule
    @JvmField
    val rule = RunTestOnContext(vertxOptions)

    private val httpClient by lazy {
        rule.vertx().createHttpClient()
    }

    private val webClient by lazy {
        WebClient.wrap(httpClient)
    }

    @Before
    fun setup() {
        setCordappPackages("net.corda.finance.contracts.asset", "net.corda.finance.contracts", "net.corda.vertxdemo")
    }

    @After
    fun tearDown() {
        webClient.close()
        unsetCordappPackages()
    }

    @Test
    fun `that using web api, we can issue cash, and receive notifications`(context: TestContext) {
        val bankUser = User("user1", "test", permissions = setOf(
            FlowPermissions.startFlowPermission<CashIssueFlow>(),
            FlowPermissions.startFlowPermission<CashPaymentFlow>()))
        val notaryFuture = startNode(DUMMY_NOTARY.name, advertisedServices = setOf(ServiceInfo(SimpleNotaryService.type)))
        val nodeAFuture = startNode(DUMMY_BANK_A.name, rpcUsers = listOf(bankUser))
        val nodeBFuture = startNode(DUMMY_BANK_B.name, rpcUsers = listOf(bankUser))
        val (nodeA, nodeB) = listOf(nodeAFuture, nodeBFuture, notaryFuture).map { it.getOrThrow() }

        nodeA.internals.registerCustomSchemas(setOf(CashSchemaV1))
        nodeB.internals.registerCustomSchemas(setOf(CashSchemaV1))

        val async = context.async()
        BANK_A_URL.websocket<List<IssuanceEvent>>("/api/events/issuance").subscribe({
            println("received issuance notification: ${Json.encode(it)}")
            async.complete()
        }, context::fail)
        BANK_A_URL.post("/api/issuance", IssueCashRequest(100)).decode<String>()
            .onSuccess {
                println(it)
            }.onFail {
            println(it.message)
        }.setHandler(context.asyncAssertSuccess())
    }

    private inline fun <reified T : Any> URI.post(path: String, body: T): Future<HttpResponse<Buffer>> {
        val result = future<HttpResponse<Buffer>>()
        webClient.post(this.port, this.host, path)
            .sendJson(body, result.completer())
        return result.map {
            if (it.statusCode() / 100 != 2) {
                throw RuntimeException("POST $path failed: ${it.statusMessage()}")
            }
            it
        }
    }

    private inline fun  <reified T: Any> URI.websocket(path: String): Observable<T> {
        return Observable.create { subscriber ->
            httpClient.websocket(port, host, path) { webSocket ->
                webSocket.handler { it ->
                    val decoded = Json.decodeValue(it, T::class.java)
                    if (subscriber.isUnsubscribed) {
                        webSocket.close()
                    } else {
                        subscriber.onNext(decoded)
                    }
                }
            }
        }
    }
}
























