package net.corda.node.services.rpc

import net.corda.client.rpc.CordaRPCClient
import net.corda.client.rpc.GracefulReconnect
import net.corda.client.rpc.internal.ReconnectingCordaRPCOps
import net.corda.client.rpc.notUsed
import net.corda.core.contracts.Amount
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.node.services.Vault
import net.corda.core.node.services.vault.*
import net.corda.core.utilities.*
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.schemas.CashSchemaV1
import net.corda.node.services.Permissions
import net.corda.node.services.rpc.RpcReconnectTests.Companion.NUMBER_OF_FLOWS_TO_RUN
import net.corda.testing.core.DUMMY_BANK_A_NAME
import net.corda.testing.core.DUMMY_BANK_B_NAME
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.OutOfProcess
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.OutOfProcessImpl
import net.corda.testing.driver.internal.incrementalPortAllocation
import net.corda.testing.node.User
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.time.Instant
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.math.absoluteValue
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.currentStackTrace

/**
 * This is a stress test for the rpc reconnection logic, which triggers failures in a probabilistic way.
 *
 * You can adjust the variable [NUMBER_OF_FLOWS_TO_RUN] to adjust the number of flows to run and the duration of the test.
 */
class RpcReconnectTests {

    companion object {
        // 150 flows take ~5 minutes
        const val NUMBER_OF_FLOWS_TO_RUN = 150

        private val log = contextLogger()
    }

    private val vaultReconciliation = VaultReconciliation()
    private val observedPennies: MutableSet<Long> = Collections.synchronizedSet(mutableSetOf<Long>())

    private val portAllocator = incrementalPortAllocation()

    private lateinit var proxy: RandomFailingProxy
    private lateinit var node: NodeHandle
    private lateinit var currentAddressPair: AddressPair

    /**
     * This test showcases and stress tests the demo [ReconnectingCordaRPCOps].
     *
     * Note that during node failure events can be lost and starting flows can become unreliable.
     * The only available way to retry failed flows is to attempt a "logical retry" which is also showcased.
     *
     * This test runs flows in a loop and in the background kills the node or restarts it.
     * Also the RPC connection is made through a proxy that introduces random latencies and is also periodically killed.
     */
    @Test
    fun `test that the RPC client is able to reconnect and proceed after node failure, restart, or connection reset`() {
        val nodeRunningTime = { Random().nextInt(12000) + 8000 }

        val demoUser = User("demo", "demo", setOf(Permissions.all()))

        // When this reaches 0 - the test will end.
        val flowsCountdownLatch = CountDownLatch(NUMBER_OF_FLOWS_TO_RUN)
        // These are the expected progress steps for the CashIssueAndPayFlow.
        val expectedProgress = listOf(
                "Starting",
                "Issuing cash",
                "Generating transaction",
                "Signing transaction",
                "Finalising transaction",
                "Broadcasting transaction to participants",
                "Paying recipient",
                "Generating anonymous identities",
                "Generating transaction",
                "Signing transaction",
                "Finalising transaction",
                "Requesting signature by notary service",
                "Requesting signature by Notary service",
                "Validating response from Notary service",
                "Broadcasting transaction to participants",
                "Done"
        )

        driver(DriverParameters(cordappsForAllNodes = FINANCE_CORDAPPS, startNodesInProcess = false, inMemoryDB = false)) {
            fun startBankA(address: NetworkHostAndPort) = startNode(providedName = DUMMY_BANK_A_NAME, rpcUsers = listOf(demoUser), customOverrides = mapOf("rpcSettings.address" to address.toString()))
            fun startProxy(addressPair: AddressPair) = RandomFailingProxy(serverPort = addressPair.proxyAddress.port, remotePort = addressPair.nodeAddress.port).start()

            val addresses = (1..2).map { getRandomAddressPair() }
            currentAddressPair = addresses[0]

            proxy = startProxy(currentAddressPair)
            val (bankA, bankB) = listOf(
                    startBankA(currentAddressPair.nodeAddress),
                    startNode(providedName = DUMMY_BANK_B_NAME, rpcUsers = listOf(demoUser))
            ).transpose().getOrThrow()
            node = bankA

            val notary = defaultNotaryIdentity
            val baseAmount = Amount.parseCurrency("0 USD")
            val issuerRef = OpaqueBytes.of(0x01)

            val addressesForRpc = addresses.map { it.proxyAddress }
            // DOCSTART rpcReconnectingRPC
            val reconnect = GracefulReconnect(onReconnect = {
                assertThat(currentStackTrace().size).isLessThan(70).withFailMessage("Reconnect logic has stack levels looking like infinite recursion. May lead to an overflow?")
                vaultReconciliation.reconcile { observedPennies.add(it.state.data.amount.quantity) }
            })
            val client = CordaRPCClient(addressesForRpc)
            val bankAReconnectingRpc = client.start(demoUser.username, demoUser.password, gracefulReconnect = reconnect).proxy as ReconnectingCordaRPCOps
            vaultReconciliation.rpc = bankAReconnectingRpc
            // DOCEND rpcReconnectingRPC

            // Observe the vault and collect the observations.
            val vaultEvents = Collections.synchronizedList(mutableListOf<Vault.Update<Cash.State>>())
            // DOCSTART rpcReconnectingRPCVaultTracking
            val vaultFeed = bankAReconnectingRpc.vaultTrackByWithPagingSpec(
                    Cash.State::class.java,
                    QueryCriteria.VaultQueryCriteria(),
                    PageSpecification(1, 1))
            val vaultSubscription = vaultFeed.updates.subscribe { update: Vault.Update<Cash.State> ->
                log.info("vault update produced ${update.produced.map { it.state.data.amount }} consumed ${update.consumed.map { it.ref }}")
                for (event in update.produced + update.consumed) {
                    vaultReconciliation.updateLastHandledTimestamp(bankAReconnectingRpc, event.ref)
                    observedPennies.add(event.state.data.amount.quantity)
                }
                vaultEvents.add(update)
            }
            // DOCEND rpcReconnectingRPCVaultTracking

            // Observe the stateMachine and collect the observations.
            val stateMachineEvents = Collections.synchronizedList(mutableListOf<StateMachineUpdate>())
            val stateMachineSubscription = bankAReconnectingRpc.stateMachinesFeed().updates.subscribe { update ->
                log.info(update.toString())
                stateMachineEvents.add(update)
            }

            // While the flows are running, randomly apply a different failure scenario.
            val nrRestarts = AtomicInteger()
            thread(name = "Node killer") {
                while (true) {
                    if (flowsCountdownLatch.count == 0L) break

                    // Let the node run for a random time interval.
                    nodeRunningTime().also { ms ->
                        log.info("Running node for ${ms / 1000} s.")
                        Thread.sleep(ms.toLong())
                    }

                    if (flowsCountdownLatch.count == 0L) break
                    when (Random().nextInt().rem(7).absoluteValue) {
                        0 -> {
                            log.info("Forcefully killing node and proxy.")
                            (node as OutOfProcessImpl).onStopCallback()
                            (node as OutOfProcess).process.destroyForcibly()
                            proxy.stop()
                            node = startBankA(currentAddressPair.nodeAddress).get()
                            proxy.start()
                        }
                        1 -> {
                            log.info("Forcefully killing node.")
                            (node as OutOfProcessImpl).onStopCallback()
                            (node as OutOfProcess).process.destroyForcibly()
                            node = startBankA(currentAddressPair.nodeAddress).get()
                        }
                        2 -> {
                            log.info("Shutting down node.")
                            node.stop()
                            proxy.stop()
                            node = startBankA(currentAddressPair.nodeAddress).get()
                            proxy.start()
                        }
                        3, 4 -> {
                            log.info("Killing proxy.")
                            proxy.stop()
                            Thread.sleep(Random().nextInt(5000).toLong())
                            proxy.start()
                        }
                        5 -> {
                            log.info("Dropping connection.")
                            proxy.failConnection()
                        }
                        6 -> {
                            log.info("Performing failover to a different node")
                            node.stop()
                            proxy.stop()
                            currentAddressPair = (addresses - currentAddressPair).first()
                            node = startBankA(currentAddressPair.nodeAddress).get()
                            proxy = startProxy(currentAddressPair)
                        }
                    }
                    nrRestarts.incrementAndGet()
                }
            }

            // Start nrOfFlowsToRun and provide a logical retry function that checks the vault.
            val flowProgressEvents = mutableMapOf<StateMachineRunId, MutableList<String>>()
            for (amount in (1..NUMBER_OF_FLOWS_TO_RUN)) {
                // DOCSTART rpcReconnectingRPCFlowStarting
                bankAReconnectingRpc.runFlowWithLogicalRetry(
                        runFlow = { rpc ->
                            log.info("Starting CashIssueAndPaymentFlow for $amount")
                            val flowHandle = rpc.startTrackedFlowDynamic(
                                    CashIssueAndPaymentFlow::class.java,
                                    baseAmount.plus(Amount.parseCurrency("$amount USD")),
                                    issuerRef,
                                    bankB.nodeInfo.legalIdentities.first(),
                                    false,
                                    notary
                            )
                            val flowId = flowHandle.id
                            log.info("Started flow $amount with flowId: $flowId")
                            flowProgressEvents.addEvent(flowId, null)

                            flowHandle.stepsTreeFeed?.updates?.notUsed()
                            flowHandle.stepsTreeIndexFeed?.updates?.notUsed()
                            // No reconnecting possible.
                            flowHandle.progress.subscribe(
                                    { prog ->
                                        flowProgressEvents.addEvent(flowId, prog)
                                        log.info("Progress $flowId : $prog")
                                    },
                                    { error ->
                                        log.error("Error thrown in the flow progress observer", error)
                                    })
                            flowHandle.id
                        },
                        hasFlowStarted = { rpc ->
                            // Query for a state that is the result of this flow.
                            val criteria = QueryCriteria.VaultCustomQueryCriteria(builder { CashSchemaV1.PersistentCashState::pennies.equal(amount.toLong() * 100) }, status = Vault.StateStatus.ALL)
                            val results = rpc.vaultQueryByCriteria(criteria, Cash.State::class.java)
                            log.info("$amount - Found states ${results.states}")
                            // The flow has completed if a state is found
                            results.states.isNotEmpty()
                        },
                        onFlowConfirmed = {
                            flowsCountdownLatch.countDown()
                            log.info("Flow started for $amount. Remaining flows: ${flowsCountdownLatch.count}")
                        }
                )
                // DOCEND rpcReconnectingRPCFlowStarting

                Thread.sleep(Random().nextInt(250).toLong())
            }

            log.info("Started all flows")

            // Wait until all flows have been started.
            val flowsConfirmed = flowsCountdownLatch.await(10, TimeUnit.MINUTES)

            if (flowsConfirmed) {
                log.info("Confirmed all flows have started.")
            } else {
                log.info("Timed out waiting for confirmation that all flows have started. Remaining flows: ${flowsCountdownLatch.count}")
            }


            // Wait for all events to come in and flows to finish.
            Thread.sleep(4000)

            val nrFailures = nrRestarts.get()
            log.info("Checking results after $nrFailures restarts.")

            // Query the vault and check that states were created for all flows.
            fun readCashStates() = bankAReconnectingRpc
                    .vaultQueryByWithPagingSpec(Cash.State::class.java, QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.CONSUMED), PageSpecification(1, 10000))
                    .states

            var allCashStates = readCashStates()
            var nrRetries = 0

            // It might be necessary to wait more for all events to arrive when the node is slow.
            while (allCashStates.size < NUMBER_OF_FLOWS_TO_RUN && nrRetries++ < 50) {
                Thread.sleep(2000)
                allCashStates = readCashStates()
            }

            val allCash = allCashStates.map { it.state.data.amount.quantity }.toSet()
            val missingCash = (1..NUMBER_OF_FLOWS_TO_RUN).filterNot { allCash.contains(it.toLong() * 100) }
            log.info("Missing cash states: $missingCash")

            assertEquals(NUMBER_OF_FLOWS_TO_RUN, allCashStates.size, "Not all flows were executed successfully")

            // The progress status for each flow can only miss the last events, because the node might have been killed.
            val missingProgressEvents = flowProgressEvents.filterValues { expectedProgress.subList(0, it.size) != it }
            assertTrue(missingProgressEvents.isEmpty(), "The flow progress tracker is missing events: $missingProgressEvents")

            // DOCSTART missingVaultEvents
            // Check that enough vault events were received.
            // This check is fuzzy because events can go missing during node restarts.
            // Ideally there should be nrOfFlowsToRun events receive but some might get lost for each restart.
            assertThat(vaultEvents!!.size + nrFailures * 3).isGreaterThanOrEqualTo(NUMBER_OF_FLOWS_TO_RUN)
            // DOCEND missingVaultEvents

            // Check that no flow was triggered twice.
            val duplicates = allCashStates.groupBy { it.state.data.amount }.filterValues { it.size > 1 }
            assertTrue(duplicates.isEmpty(), "${duplicates.size} flows were retried illegally.")

            log.info("State machine events seen: ${stateMachineEvents!!.size}")
            // State machine events are very likely to get lost more often because they seem to be sent with a delay.
            assertThat(stateMachineEvents.count { it is StateMachineUpdate.Added }).isGreaterThanOrEqualTo(NUMBER_OF_FLOWS_TO_RUN / 3)
            assertThat(stateMachineEvents.count { it is StateMachineUpdate.Removed }).isGreaterThanOrEqualTo(NUMBER_OF_FLOWS_TO_RUN / 3)

            // Stop the observers.
            vaultSubscription.unsubscribe()
            stateMachineSubscription.unsubscribe()
            bankAReconnectingRpc.close()
        }

        proxy.close()
    }

    @Synchronized
    fun MutableMap<StateMachineRunId, MutableList<String>>.addEvent(id: StateMachineRunId, progress: String?): Boolean {
        return getOrPut(id) { mutableListOf() }.let { if (progress != null) it.add(progress) else false }
    }
    private fun getRandomAddressPair() = AddressPair(getRandomAddress(), getRandomAddress())
    private fun getRandomAddress() = NetworkHostAndPort("localhost", portAllocator.nextPort())

    data class AddressPair(val proxyAddress: NetworkHostAndPort, val nodeAddress: NetworkHostAndPort)
}

class VaultReconciliation {

    companion object {
        /**
         * Vault updates can be observed out of temporal order. When there is a need to reconcile, we query the vault for states
         * that have been recorded since the recorded timestamp of the last handled event minus this duration.
         */
        val temporalOrderingUncertainty = 1.hours
    }

    @Volatile
    var lastRecordedTime: Instant = Instant.now()

    lateinit var rpc: CordaRPCOps

    /**
     * This method supposed to be passed into the `onReconnect` parameter of the ReconnectingRPCClient.
     *
     * On reconnect we fetch all states from the vault that have been recorded since the recorded timestamp of the last handled event minus
     * the temporal ordering uncertainty defined above. We are subtracting this duration from the last recorded timestamp since the
     * vault updates are not guaranteed to be observed in recorded timestamp order.
     *
     * Note that vault pages are fetched in independent database transactions and there might be states missing under certain conditions
     * when the vault records new states while we are fetching pages, especially if the node clock that determines the recorded timestamp
     * is jumping backwards.
     *
     * In some cases reconcile might need to use an internal buffer of handled states, to avoid calling the handler function more than once
     * for a given state. This depends on the requirements of the application. In this test we are simply adding each state to a set, therefore
     * it's not a problem to handle a state several times.
     */
    fun reconcile(onNewState: (StateAndRef<Cash.State>) -> Unit) {
        val beforeLastDelivery = lastRecordedTime.minus(temporalOrderingUncertainty)
        val upperTimeBound = Instant.now()
        val timeCondition = QueryCriteria.TimeCondition(QueryCriteria.TimeInstantType.RECORDED, ColumnPredicate.Between(beforeLastDelivery, upperTimeBound))
        val queryCriteria = QueryCriteria.VaultQueryCriteria(Vault.StateStatus.ALL, timeCondition = timeCondition)
        val sorting = Sort(setOf(Sort.SortColumn(SortAttribute.Standard(Sort.VaultStateAttribute.RECORDED_TIME), Sort.Direction.ASC)))
        var pageNumber = 1
        val pageSize = 1000
        do {
            val paging = PageSpecification(pageNumber, pageSize)
            val snapshot = rpc.vaultQueryBy(queryCriteria, paging, sorting, Cash.State::class.java)
            for (state in snapshot.states) {
                onNewState(state)
                updateLastHandledTimestamp(rpc, state.ref)
            }
            pageNumber++
        } while (pageSize * (pageNumber - 1) < snapshot.totalStatesAvailable)
    }

    // TODO: As an optimisation, we could add a second method that operates on `Vault.Update`s, querying the vault
    // for all the states in the update and updating the timestamp to the maximum recorded time.
    fun updateLastHandledTimestamp(rpc: CordaRPCOps, ref: StateRef) {
        val recordedTime = rpc.vaultQueryByCriteria(
                QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL, stateRefs = listOf(ref)),
                Cash.State::class.java).statesMetadata.single().recordedTime
        lastRecordedTime = recordedTime
    }
}
