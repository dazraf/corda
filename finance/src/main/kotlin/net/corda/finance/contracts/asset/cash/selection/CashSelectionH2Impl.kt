package net.corda.finance.contracts.asset.cash.selection

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.Amount
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TransactionState
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FlowLogic
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.StatesNotAvailableException
import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.deserialize
import net.corda.core.utilities.*
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.contracts.asset.CashSelection
import java.sql.DatabaseMetaData
import java.sql.SQLException
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class CashSelectionH2Impl : CashSelection {

    companion object {
        const val JDBC_DRIVER_NAME = "H2 JDBC Driver"
        val log = loggerFor<CashSelectionH2Impl>()
    }

    override fun isCompatible(metadata: DatabaseMetaData): Boolean {
        return metadata.driverName == JDBC_DRIVER_NAME
    }

    // coin selection retry loop counter, sleep (msecs) and lock for selecting states
    private val MAX_RETRIES = 8
    private val RETRY_SLEEP = 100
    private val RETRY_CAP = 2000
    private val spendLock: ReentrantLock = ReentrantLock()

    /**
     * An optimised query to gather Cash states that are available and retry if they are temporarily unavailable.
     * @param services The service hub to allow access to the database session
     * @param amount The amount of currency desired (ignoring issues, but specifying the currency)
     * @param onlyFromIssuerParties If empty the operation ignores the specifics of the issuer,
     * otherwise the set of eligible states wil be filtered to only include those from these issuers.
     * @param notary If null the notary source is ignored, if specified then only states marked
     * with this notary are included.
     * @param lockId The FlowLogic.runId.uuid of the flow, which is used to soft reserve the states.
     * Also, previous outputs of the flow will be eligible as they are implicitly locked with this id until the flow completes.
     * @param withIssuerRefs If not empty the specific set of issuer references to match against.
     * @return The matching states that were found. If sufficient funds were found these will be locked,
     * otherwise what is available is returned unlocked for informational purposes.
     */
    @Suspendable
    override fun unconsumedCashStatesForSpending(services: ServiceHub,
                                                 amount: Amount<Currency>,
                                                 onlyFromIssuerParties: Set<AbstractParty>,
                                                 notary: Party?,
                                                 lockId: UUID,
                                                 withIssuerRefs: Set<OpaqueBytes>): List<StateAndRef<Cash.State>> {

        val stateAndRefs = mutableListOf<StateAndRef<Cash.State>>()

        for (retryCount in 1..MAX_RETRIES) {
            if (!attemptSpend(services, amount, lockId, notary, onlyFromIssuerParties, withIssuerRefs, stateAndRefs)) {
                log.warn("Coin selection failed on attempt $retryCount")
                // TODO: revisit the back off strategy for contended spending.
                if (retryCount != MAX_RETRIES) {
                    stateAndRefs.clear()
                    val durationMillis = (minOf(RETRY_SLEEP.shl(retryCount), RETRY_CAP / 2) * (1.0 + Math.random())).toInt()
                    FlowLogic.sleep(durationMillis.millis)
                } else {
                    log.warn("Insufficient spendable states identified for $amount")
                }
            } else {
                break
            }
        }
        return stateAndRefs
    }

    //       We are using an H2 specific means of selecting a minimum set of rows that match a request amount of coins:
    //       1) There is no standard SQL mechanism of calculating a cumulative total on a field and restricting row selection on the
    //          running total of such an accumulator
    //       2) H2 uses session variables to perform this accumulator function:
    //          http://www.h2database.com/html/functions.html#set
    //       3) H2 does not support JOIN's in FOR UPDATE (hence we are forced to execute 2 queries)

    private fun attemptSpend(services: ServiceHub, amount: Amount<Currency>, lockId: UUID, notary: Party?, onlyFromIssuerParties: Set<AbstractParty>, withIssuerRefs: Set<OpaqueBytes>, stateAndRefs: MutableList<StateAndRef<Cash.State>>): Boolean {
        val connection = services.jdbcSession()
        spendLock.withLock {
            val statement = connection.createStatement()
            try {
                statement.execute("CALL SET(@t, CAST(0 AS BIGINT));")

                val selectJoin = """
                    SELECT vs.transaction_id, vs.output_index, vs.contract_state, ccs.pennies, SET(@t, ifnull(@t,0)+ccs.pennies) total_pennies, vs.lock_id
                    FROM vault_states AS vs, contract_cash_states AS ccs
                    WHERE vs.transaction_id = ccs.transaction_id AND vs.output_index = ccs.output_index
                    AND vs.state_status = 0
                    AND ccs.ccy_code = ? and @t < ?
                    AND (vs.lock_id = ? OR vs.lock_id is null)
                    """ +
                        (if (notary != null)
                            " AND vs.notary_name = ?" else "") +
                        (if (onlyFromIssuerParties.isNotEmpty())
                            " AND ccs.issuer_key IN (?)" else "") +
                        (if (withIssuerRefs.isNotEmpty())
                            " AND ccs.issuer_ref IN (?)" else "")

                // Use prepared statement for protection against SQL Injection (http://www.h2database.com/html/advanced.html#sql_injection)
                val psSelectJoin = connection.prepareStatement(selectJoin)
                var pIndex = 0
                psSelectJoin.setString(++pIndex, amount.token.currencyCode)
                psSelectJoin.setLong(++pIndex, amount.quantity)
                psSelectJoin.setString(++pIndex, lockId.toString())
                if (notary != null)
                    psSelectJoin.setString(++pIndex, notary.name.toString())
                if (onlyFromIssuerParties.isNotEmpty())
                    psSelectJoin.setObject(++pIndex, onlyFromIssuerParties.map { it.owningKey.toBase58String() as Any}.toTypedArray() )
                if (withIssuerRefs.isNotEmpty())
                    psSelectJoin.setObject(++pIndex, withIssuerRefs.map { it.bytes.toHexString() as Any }.toTypedArray())
                log.debug { psSelectJoin.toString() }

                // Retrieve spendable state refs
                val rs = psSelectJoin.executeQuery()
                stateAndRefs.clear()
                var totalPennies = 0L
                while (rs.next()) {
                    val txHash = SecureHash.parse(rs.getString(1))
                    val index = rs.getInt(2)
                    val stateRef = StateRef(txHash, index)
                    val state = rs.getBytes(3).deserialize<TransactionState<Cash.State>>(context = SerializationDefaults.STORAGE_CONTEXT)
                    val pennies = rs.getLong(4)
                    totalPennies = rs.getLong(5)
                    val rowLockId = rs.getString(6)
                    stateAndRefs.add(StateAndRef(state, stateRef))
                    log.trace { "ROW: $rowLockId ($lockId): $stateRef : $pennies ($totalPennies)" }
                }

                if (stateAndRefs.isNotEmpty() && totalPennies >= amount.quantity) {
                    // we should have a minimum number of states to satisfy our selection `amount` criteria
                    log.trace("Coin selection for $amount retrieved ${stateAndRefs.count()} states totalling $totalPennies pennies: $stateAndRefs")

                    // With the current single threaded state machine available states are guaranteed to lock.
                    // TODO However, we will have to revisit these methods in the future multi-threaded.
                    services.vaultService.softLockReserve(lockId, (stateAndRefs.map { it.ref }).toNonEmptySet())
                    return true
                }
                log.trace("Coin selection requested $amount but retrieved $totalPennies pennies with state refs: ${stateAndRefs.map { it.ref }}")
                // retry as more states may become available
            } catch (e: SQLException) {
                log.error("""Failed retrieving unconsumed states for: amount [$amount], onlyFromIssuerParties [$onlyFromIssuerParties], notary [$notary], lockId [$lockId]
                            $e.
                        """)
            } catch (e: StatesNotAvailableException) { // Should never happen with single threaded state machine
                log.warn(e.message)
                // retry only if there are locked states that may become available again (or consumed with change)
            } finally {
                statement.close()
            }
        }
        return false
    }
}