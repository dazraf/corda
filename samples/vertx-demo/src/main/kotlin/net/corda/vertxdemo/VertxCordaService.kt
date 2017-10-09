package net.corda.vertxdemo

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.ServiceHubInternal

/**
 * A CordaService to bootstrap vertx and a single
 * [verticle](http://vertx.io/docs/vertx-core/java/#_verticles), [MyVerticle]
 *
 * This class is instantiated by the Corda runtime
 * It:
 *  1. creates and holds a reference to [Vertx]
 *  2. instantiates a vertx verticle and deploys it
 *
 * This class can be altered to dynamically load multiple verticles - an exercise for the reader
 */
@CordaService
@Suppress("unused") // compiler hint: loaded dynamically by the  Corda node
class VertxCordaService(hub: ServiceHub, private val configPath: String) : SingletonSerializeAsToken() {
    companion object {
        val logger = loggerFor<VertxCordaService>()
        init {
            Json.mapper.registerModule(KotlinModule())
            Json.prettyMapper.registerModule(KotlinModule())
        }
    }

    @Suppress("unused") // compiler hint: called dynamically by the Corda node
    constructor(hub: ServiceHub) : this(hub, "vertx-config.json")

    /**
     * accessing [ServiceHubInternal] until this issue is resolved:
     * https://github.com/corda/corda/issues/1479
     */
    private val serviceHub = hub as ServiceHubInternal
    private val vertx = Vertx.vertx()

    init {
        val me = serviceHub.myInfo.legalIdentities.first()

        val vertxConfig = readVertxConfig()
        val orgConfig = vertxConfig.getJsonObject("config")
            ?.getJsonObject("nodes")
            ?.getJsonObject(me.name.organisation)
        if (orgConfig != null) {
            vertx.deployVerticle(MyVerticle(serviceHub), DeploymentOptions(vertxConfig))
            // insert other verticles here ...
        } else {
            logger.warn("did not find any config for ${me.name}")
        }
    }

    private fun readVertxConfig(): JsonObject {
        val resource = VertxCordaService::class.java.classLoader.getResource(configPath)
        if (resource == null) {
            logger.warn("could not find $configPath")
        } else {
            logger.info("found $configPath")
        }
        return JsonObject(resource?.readText() ?: "{}")
    }
}


