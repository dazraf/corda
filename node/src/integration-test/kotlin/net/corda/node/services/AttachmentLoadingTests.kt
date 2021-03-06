package net.corda.node.services

import net.corda.client.rpc.RPCException
import net.corda.core.contracts.Contract
import net.corda.core.contracts.PartyAndReference
import net.corda.core.cordapp.CordappProvider
import net.corda.core.flows.FlowLogic
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.internal.toLedgerTransaction
import net.corda.core.serialization.SerializationFactory
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.loggerFor
import net.corda.node.internal.cordapp.CordappLoader
import net.corda.node.internal.cordapp.CordappProviderImpl
import net.corda.nodeapi.User
import net.corda.testing.DUMMY_BANK_A
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.TestDependencyInjectionBase
import net.corda.testing.driver.driver
import net.corda.testing.node.MockServices
import net.corda.testing.resetTestSerialization
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.net.URLClassLoader
import java.nio.file.Files
import kotlin.test.assertFailsWith

class AttachmentLoadingTests : TestDependencyInjectionBase() {
    private class Services : MockServices() {
        private val provider = CordappProviderImpl(CordappLoader.createDevMode(listOf(isolatedJAR))).start(attachments)
        private val cordapp get() = provider.cordapps.first()
        val attachmentId get() = provider.getCordappAttachmentId(cordapp)!!
        val appContext get() = provider.getAppContext(cordapp)
        override val cordappProvider: CordappProvider = provider
    }

    private companion object {
        val logger = loggerFor<AttachmentLoadingTests>()
        val isolatedJAR = AttachmentLoadingTests::class.java.getResource("isolated.jar")!!
        val ISOLATED_CONTRACT_ID = "net.corda.finance.contracts.isolated.AnotherDummyContract"
    }

    private lateinit var services: Services

    @Before
    fun setup() {
        services = Services()
    }

    @Test
    fun `test a wire transaction has loaded the correct attachment`() {
        val appClassLoader = services.appContext.classLoader
        val contractClass = appClassLoader.loadClass(ISOLATED_CONTRACT_ID).asSubclass(Contract::class.java)
        val generateInitialMethod = contractClass.getDeclaredMethod("generateInitial", PartyAndReference::class.java, Integer.TYPE, Party::class.java)
        val contract = contractClass.newInstance()
        val txBuilder = generateInitialMethod.invoke(contract, PartyAndReference(DUMMY_BANK_A, OpaqueBytes(kotlin.ByteArray(1))), 1, DUMMY_NOTARY) as TransactionBuilder
        val context = SerializationFactory.defaultFactory.defaultContext
            .withClassLoader(appClassLoader)
        val ledgerTx = txBuilder.toLedgerTransaction(services, context)
        contract.verify(ledgerTx)

        val actual = ledgerTx.attachments.first()
        val expected = services.attachments.openAttachment(services.attachmentId)!!

        assertEquals(expected, actual)
    }

    @Test
    fun `test that attachments retrieved over the network are not used for code`() {
        driver(initialiseSerialization = false) {
            val bankAName = CordaX500Name("BankA", "Zurich", "CH")
            val bankBName = CordaX500Name("BankB", "Zurich", "CH")
            // Copy the app jar to the first node. The second won't have it.
            val path = (baseDirectory(bankAName.toString()) / "plugins").createDirectories() / "isolated.jar"
            logger.info("Installing isolated jar to $path")
            isolatedJAR.openStream().buffered().use { input ->
                Files.newOutputStream(path).buffered().use { output ->
                    input.copyTo(output)
                }
            }

            val admin = User("admin", "admin", permissions = setOf("ALL"))
            val (bankA, bankB) = listOf(
                    startNode(providedName = bankAName, rpcUsers = listOf(admin)),
                    startNode(providedName = bankBName, rpcUsers = listOf(admin))
            ).transpose().getOrThrow()   // Wait for all nodes to start up.

            val clazz =
                    Class.forName("net.corda.finance.contracts.isolated.IsolatedDummyFlow\$Initiator", true, URLClassLoader(arrayOf(isolatedJAR)))
                            .asSubclass(FlowLogic::class.java)

            try {
                bankA.rpcClientToNode().start("admin", "admin").use { rpc ->
                    val proxy = rpc.proxy
                    val party = proxy.wellKnownPartyFromX500Name(bankBName)!!

                    assertFailsWith<RPCException>("net.corda.client.rpc.RPCException: net.corda.finance.contracts.isolated.IsolatedDummyFlow\$Initiator") {
                        proxy.startFlowDynamic(clazz, party).returnValue.getOrThrow()
                    }
                }
            } finally {
                bankA.stop()
                bankB.stop()
            }
        }
    }
}
