package net.corda.vertxdemo

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Future.future
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.client.HttpResponse
import net.corda.core.internal.FlowStateMachine
import net.corda.core.utilities.getOrThrow
import javax.json.JsonObject


// Useful utility methods for vertx, kotlin, etc

/**
 * convert a FlowStateMachine (the result of a flow invocation) to a vertx [Future]
 */
fun <T : Any> FlowStateMachine<T>.asVertxFuture(vertx: Vertx): Future<T> {
    val result = future<T>()
    this.resultFuture.then { future ->
        if (future.isDone) {
            vertx.runOnContext {
                try {
                    result.complete(future.getOrThrow())
                } catch (err: Throwable) {
                    result.fail(err)
                }
            }
        }
    }
    return result
}

/**
 * Utility to write the result of an async execution to a Http server response.
 * This handles the case where the async operation has failed, reporting an suitable HTTP response.
 */
fun <T : Any> HttpServerResponse.end(payload: AsyncResult<T>) {
    if (payload.succeeded()) {
        this.end(payload.result())
    } else {
        statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR.code()
        statusMessage = payload.cause().message
        end()
    }
}


/**
 * Respond on a HTTP reqquest, with an object represented as a JSON payload
 */
fun <T : Any> HttpServerResponse.end(payload: T) {
    putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    end(Json.encode(payload))
}

/**
 * Tap this future for the happy path, executing a function.
 * If the function fails, the chained future fails also.
 */
fun <T : Any> Future<T>.onSuccess(fn: (T) -> Unit): Future<T> {
    val result = future<T>()
    this.setHandler {
        with(it) {
            if (failed()) {
                result.fail(cause())
            } else {
                try {
                    fn(result())
                    result.complete(result())
                } catch (err: Throwable) {
                    result.fail(err)
                }
            }
        }
    }
    return result
}

/**
 * Tap this future for the unhappy path, executing a function, and if succecssful, passing on the exception down the chain.
 * If the function [fn] fails, the chained future fails with the raised exception from the function.
 */
fun <T : Any> Future<T>.onFail(fn: (Throwable) -> Unit): Future<T> {
    val result = future<T>()
    this.setHandler {
        with(it) {
            if (failed()) {
                try {
                    fn(cause())
                    result.fail(cause())
                } catch (err: Throwable) {
                    result.fail(err)
                }
            } else {
                result.complete(result())
            }
        }
    }
    return result
}

/**
 * Decode a HTTP request body as a strong type [T]
 */
inline fun <reified T: Any >RoutingContext.decodeBody(): T {
    return Json.decodeValue(this.body, T::class.java)
}

/**
 * Chain the response from a [HttpResponse] [Future], decoding to a strong type [T]
 */
inline fun <reified T : Any> Future<HttpResponse<Buffer>>.decode(): Future<T> {
    return this.map {
        when (T::class) {
            String::class -> {
                it.bodyAsString() as T
            }
            JsonObject::class -> {
                it.bodyAsJsonObject() as T
            }
            JsonArray::class -> {
                it.bodyAsJsonArray() as T
            }
            else -> {
                it.bodyAsJson(T::class.java) as T
            }
        }

    }
}


