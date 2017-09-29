package net.corda.node.services.network

import net.corda.cordform.CordformNode
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.KeyManagementService
import net.corda.node.services.identity.InMemoryIdentityService
import net.corda.testing.*
import net.corda.testing.node.MockKeyManagementService
import net.corda.testing.node.NodeBasedTest
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import rx.observers.TestSubscriber
import rx.schedulers.TestScheduler
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.contentOf

class NodeInfoSerializerTest : NodeBasedTest() {

    @Rule @JvmField var folder = TemporaryFolder()

    lateinit var keyManagementService: KeyManagementService
    val scheduler = TestScheduler();

    // Object under test
    lateinit var nodeInfoSerializer : NodeInfoSerializer

    companion object {
        val nodeInfoFileRegex = Regex("nodeInfo\\-.*")
        val nodeInfo = NodeInfo(listOf(), listOf(getTestPartyAndCertificate(ALICE)), 0, 0)
    }

    @Before
    fun start() {
        val identityService = InMemoryIdentityService(trustRoot = DEV_TRUST_ROOT)
        keyManagementService = MockKeyManagementService(identityService, ALICE_KEY)
        nodeInfoSerializer = NodeInfoSerializer(folder.root.toPath(), scheduler)
    }

    @Test
    fun `save a NodeInfo`() {
        NodeInfoSerializer.saveToFile(folder.root.toPath(), nodeInfo, keyManagementService)

        assertEquals(1, folder.root.list().size)
        val fileName = folder.root.list()[0]
        assertTrue(fileName.matches(nodeInfoFileRegex))
        val file = (folder.root.path / fileName).toFile()
        // Just check that something is written, another tests verifies that the written value can be read back.
        assertThat(contentOf(file)).isNotEmpty()
    }

    @Test
    fun `load an empty Directory`() {
        assertEquals(0, nodeInfoSerializer.loadFromDirectory().size)
    }

    @Test
    fun `load a non empty Directory`() {
        createNodeInfoFileInPath(nodeInfo)

        val nodeInfos = nodeInfoSerializer.loadFromDirectory()

        assertEquals(1, nodeInfos.size)
        assertEquals(nodeInfo, nodeInfos.first())
    }

    @Test
    fun `polling folder`() {
        val testSubscriber = TestSubscriber<NodeInfo>()
        nodeInfoSerializer.nodeInfoDirectory.createDirectories()

        // Start polling with an empty folder.
        nodeInfoSerializer.directoryObservable()
                .subscribe(testSubscriber)
        // Ensure the watch service is started.
        scheduler.advanceTimeBy(1, TimeUnit.HOURS)

        // Check no nodeInfos are read.
        assertEquals(0, testSubscriber.valueCount)
        createNodeInfoFileInPath(nodeInfo)

        scheduler.advanceTimeBy(1, TimeUnit.HOURS)

        // The same folder can be reported more than once, so take unique values.
        val readNodes = testSubscriber.onNextEvents.distinct()
        assertEquals(1, readNodes.size)
        assertEquals(nodeInfo, readNodes.first())
    }

    // Write a nodeInfo under the right path.
    private fun createNodeInfoFileInPath(nodeInfo: NodeInfo) {
        NodeInfoSerializer.saveToFile(folder.root.toPath() / CordformNode.NODE_INFO_DIRECTORY,
                nodeInfo, keyManagementService)
    }
}
