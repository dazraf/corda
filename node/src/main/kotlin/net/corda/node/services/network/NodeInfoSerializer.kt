package net.corda.node.services.network

import net.corda.cordform.CordformNode
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.internal.*
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.KeyManagementService
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.ByteSequence
import net.corda.core.utilities.loggerFor
import rx.Observable
import rx.Scheduler
import rx.schedulers.Schedulers
import java.io.File
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.nio.file.Files
import java.util.concurrent.TimeUnit

/**
 * Class containing the logic to
 * - Serialize and de-serialize a [NodeInfo] to disk and reading it back.
 * - Poll a directory for new serialized [NodeInfo]
 *
 * @param path the base path of a node.
 * @param scheduler a [Scheduler] for the rx [Observable] returned by [directoryObservable], this is mainly useful for
 *        testing.
 */
class NodeInfoSerializer(private val nodePath: Path,
                         private val scheduler: Scheduler = Schedulers.computation()) {

    @VisibleForTesting
    val nodeInfoDirectory = nodePath / CordformNode.NODE_INFO_DIRECTORY
    private val watchService : WatchService? by lazy { initWatch() }

    companion object {
        val logger = loggerFor<NodeInfoSerializer>()

        /**
         * Saves the given [NodeInfo] to a path.
         * The node is 'encoded' as a SignedData<NodeInfo>, signed with the owning key of its first identity.
         * The name of the written file will be "nodeInfo-" followed by the hash of the content. The hash in the filename
         * is used so that one can freely copy these files without fearing to overwrite another one.
         *
         * @param path the path where to write the file, if non-existent it will be created.
         * @param nodeInfo the NodeInfo to serialize.
         * @param keyManager a KeyManagementService used to sign the NodeInfo data.
         */
        fun saveToFile(path: Path, nodeInfo: NodeInfo, keyManager: KeyManagementService) {
            try {
                path.createDirectories()
                val serializedBytes = nodeInfo.serialize()
                val regSig = keyManager.sign(serializedBytes.bytes,
                        nodeInfo.legalIdentities.first().owningKey)
                val signedData = SignedData(serializedBytes, regSig)
                val file = (path / ("nodeInfo-" + SecureHash.sha256(serializedBytes.bytes).toString())).toFile()
                file.writeBytes(signedData.serialize().bytes)
            } catch (e: Exception) {
                logger.warn("Couldn't write node info to file", e)
            }
        }
    }

    /**
     * Loads all the files contained in a given path and returns the deserialized [NodeInfo]s.
     * Signatures are checked before returning a value.
     *
     * @return a list of [NodeInfo]s
     */
    fun loadFromDirectory(): List<NodeInfo> {
        val result = mutableListOf<NodeInfo>()
        val nodeInfoDirectory = nodePath / CordformNode.NODE_INFO_DIRECTORY
        if (!nodeInfoDirectory.isDirectory()) {
            logger.info("$nodeInfoDirectory isn't a Directory, not loading NodeInfo from files")
            return result
        }
        for (path in Files.list(nodeInfoDirectory)) {
            val file = path.toFile()
            if (file.isFile) {
                processFile(file)?.let {
                    result.add(it)
                }
            }
        }
        logger.info("Successfully read ${result.size} NodeInfo files.")
        return result
    }

    /**
     * @return an [Observable] returning new [NodeInfo]s, there is no guarantee that the same value isn't returned more
     *      than once.
     */
    fun directoryObservable() : Observable<NodeInfo> {
        return Observable.interval(5, TimeUnit.SECONDS, scheduler)
                .flatMapIterable { pollWatch() }
    }

    // Polls the watchService for changes to nodeInfoDirectory, return all the newly read NodeInfos.
    private fun pollWatch() : List<NodeInfo> {
        val result = ArrayList<NodeInfo>()
        val files = ArrayList<File>()
        if (watchService == null) {
            return result
        }

        val watchKey: WatchKey? = watchService!!.poll()
        // This can happen and it means that there are no events.
        if (watchKey == null) {
            return result
        }

        for (event in watchKey.pollEvents()) {
            val kind = event.kind()
            if (kind == StandardWatchEventKinds.OVERFLOW) continue

            @Suppress("UNCHECKED_CAST")  // The actual type of these events is determined by event kinds we register for.
            val ev = event as WatchEvent<Path>
            val filename = ev.context()
            val absolutePath = nodeInfoDirectory.resolve(filename)
            if (absolutePath.isRegularFile()) {
                files.add(absolutePath.toFile())
            }
        }
        val valid = watchKey.reset()
        if (!valid) {
            logger.warn("Can't poll $nodeInfoDirectory anymore, it was probably deleted.")
        }
        return files.distinct()
                .map { processFile(it) }
                .filterNotNull()
    }

    private fun processFile(file :File) : NodeInfo? {
        try {
            logger.info("Reading NodeInfo from file: $file")
            val signedData: SignedData<NodeInfo> = ByteSequence.of(file.readBytes()).deserialize()
            return signedData.verified()
        } catch (e: Exception) {
            logger.warn("Exception parsing NodeInfo from file. $file", e)
            return null
        }
    }

    // Create a WatchService watching for changes in nodeInfoDirectory.
    private fun initWatch() : WatchService? {
        if (!nodeInfoDirectory.isDirectory()) {
            logger.warn("Not watching folder $nodeInfoDirectory it doesn't exist or it's not a directory")
            return null
        }
        val watchService = nodeInfoDirectory.fileSystem.newWatchService()
        nodeInfoDirectory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY)
        logger.info("Watching $nodeInfoDirectory for new files")
        return watchService
    }
}