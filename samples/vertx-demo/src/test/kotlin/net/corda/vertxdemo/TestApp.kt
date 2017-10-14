package net.corda.vertxdemo

import net.corda.core.utilities.getOrThrow
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.nodeapi.User
import net.corda.nodeapi.internal.ServiceInfo
import net.corda.testing.DUMMY_BANK_A
import net.corda.testing.DUMMY_BANK_B
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.driver.NodeParameters
import net.corda.testing.driver.driver

/**
 * Use this class to start the stack for easy local dev debugging
 * Don't forget to add the following to the VM options of the run/debug configuration
 * -javaagent:lib/quasar.jar
 */
fun main(args: Array<String>) {
    // No permissions required as we are not invoking flows.
    val user = User("user1", "test", permissions = setOf("ALL"))
    driver(isDebug = true) {
        val nodeControllerFuture = startNode<Unit>(NodeParameters(
                providedName = DUMMY_NOTARY.name,
                startInSameProcess = true
        ))
        val nodeAFuture = startNode<Unit>(NodeParameters(
                providedName = DUMMY_BANK_A.name,
                rpcUsers = listOf(user),
                startInSameProcess = true
        ))

        val nodeBFuture = startNode<Unit>(NodeParameters(
                providedName = DUMMY_BANK_B.name,
                rpcUsers = listOf(user),
                startInSameProcess = true
        ))
        listOf(nodeAFuture, nodeBFuture, nodeControllerFuture).map { it.getOrThrow() }
        OpenLink.open("http://localhost:40000", "http://localhost:40001")
        waitForAllNodesToFinish()
    }
}

/**
 * Because [java.awt.Desktop] is buggy on some platforms
 */
class OpenLink {
    companion object {
        private val command = when (OS.CURRENT) {
            OS.WINDOWS -> "start"
            OS.OSX -> "open"
            OS.UNIX -> "xdg-open"
            OS.UNKNOWN -> "echo please open"
        }

        fun open(vararg links: String) {
            val rt = Runtime.getRuntime()
            links.forEach { rt.exec("$command $it") }
        }
    }
}

enum class OS {
    WINDOWS, OSX, UNIX, UNKNOWN;

    companion object {
        val OSNAME = System.getProperty("os.name")
        val CURRENT: OS by lazy {
            when (OSNAME.toLowerCase()) {
                in Regex("win") -> OS.WINDOWS
                in Regex("(unix)|(linux)") -> OS.UNIX
                in Regex("mac") -> OS.OSX
                else -> OS.UNKNOWN
            }
        }

        private operator fun Regex.contains(text: CharSequence) = this.matches(text)
    }
}
