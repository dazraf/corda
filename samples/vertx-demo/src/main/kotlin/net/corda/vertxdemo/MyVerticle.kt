package net.corda.vertxdemo

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.http.HttpServer
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.StaticHandler
import net.corda.core.flows.FlowInitiator
import net.corda.core.node.services.Vault
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.loggerFor
import net.corda.finance.POUNDS
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.CashIssueFlow
import net.corda.node.services.api.ServiceHubInternal
import net.corda.nodeapi.User

/**
 * This vertx service verticle demonstrates a few use-cases
 * 1. Passing in a reference to the [ServiceHubInternal]
 * 2. Exposing a typical vertx REST API end-point (without security) which allows for issuance of GBP
 * 3. Exposing a typical vertx websocket for eventing cash issuances back to the caller
 */
class MyVerticle(val serviceHub: ServiceHubInternal) : AbstractVerticle() {
    private val myIdentity = serviceHub.myInfo.legalIdentities.first()

    companion object {
        val logger = loggerFor<MyVerticle>()
    }

    override fun start(startFuture: Future<Void>) {
        val organisation = myIdentity.name.organisation
        logger.debug("config is ${config().encodePrettily()}")
        logger.debug("looking for $organisation")
        val router = setupRouter()
        val port = getPort(organisation)
        setupServer(router, port)
            .onSuccess { println("Started vertx webserver on".padEnd(40) + ": http://localhost:$port") }
            .onFail {
                println("Failed to start vertx webserver".padEnd(40) + ": ${it.message}")
                logger.error("failed to start vertx webserver", it)
            }
            .mapEmpty<Void>()
            .setHandler(startFuture::handle)
    }

    private fun setupServer(router: Router, port: Int): Future<HttpServer> {
        val result = Future.future<HttpServer>()
        vertx.createHttpServer()
            .requestHandler(router::accept)
            .websocketHandler(this::onSocket)
            .listen(port, result.completer())
        return result
    }

    private fun onSocket(socket: ServerWebSocket) {
        when (socket.path()) {
            "/api/events/issuance" -> {
                bindIssuanceEvents(socket)
            }
            else -> {
                socket.reject()
            }
        }
    }

    private fun setupRouter(): Router {
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        router.get("/api/nodes").handler { it.getNodeIdentities() }
        router.get("/api/nodes/me").handler {it.getMyIdentity() }
        router.post("/api/issuance").handler { it.issueCash(it.decodeBody()) }
        // ... other apis here ...
        router.get().handler(StaticHandler.create("vertx-web").setCachingEnabled(false).setCacheEntryTimeout(1).setMaxCacheSize(1))
        return router
    }

    private fun bindIssuanceEvents(socket: ServerWebSocket) {
        try {
            serviceHub.database.transaction {
                val subscription = serviceHub.vaultService.trackBy(Cash.State::class.java).updates.subscribe {
                    writeIssuanceEventToSocket(it, socket)
                }
                socket.closeHandler { subscription.unsubscribe() }
            }
        } catch (err: Throwable) {
            socket.reject()
            logger.error("failed to track cash states", err)
        }
    }

    private fun writeIssuanceEventToSocket(it: Vault.Update<Cash.State>, socket: ServerWebSocket) {
        val result = it.produced.map {
            with(it.state.data) {
                IssuanceEvent(party = owner.nameOrNull()?.toString() ?: "", amount = amount.quantity)
            }

        }
        socket.writeFinalTextFrame(Json.encode(result))
    }

    private fun getPort(organisation: String) =
        config().getJsonObject("nodes")?.getJsonObject(organisation)?.getInteger("port") ?: 8080



    private fun RoutingContext.issueCash(request: IssueCashRequest) {
        val notary = serviceHub.networkMapCache.notaryIdentities.first()
        serviceHub.startFlow(CashIssueFlow(request.amount.POUNDS, OpaqueBytes.of(0), notary),
            FlowInitiator.RPC(primaryUser.username)).asVertxFuture(vertx)
            .map { it.stx.id.toString() }
            .setHandler { response().end(it) }
    }

    private fun RoutingContext.getNodeIdentities() {
        val identities = serviceHub.networkMapCache.allNodes
            .filter { !serviceHub.networkMapCache.isNotary(it.legalIdentities.first()) }
            .map { it.legalIdentities.first().name.toString() }
        response().end(identities)
    }

    private fun RoutingContext.getMyIdentity() {
        response().end(serviceHub.myInfo.legalIdentities.first().name)
    }

    private val primaryUser : User get() = serviceHub.configuration.rpcUsers.first()
}

data class IssueCashRequest(val amount: Int)
data class IssuanceEvent(val party: String, val amount: Long)