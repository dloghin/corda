package net.corda.node.internal.djvm

import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializationFactory
import net.corda.core.serialization.SerializedBytes
import net.corda.core.utilities.ByteSequence
import net.corda.djvm.rewiring.SandboxClassLoader
import net.corda.djvm.serialization.createSandboxSerializationEnv
import net.corda.djvm.serialization.deserializeTo
import net.corda.node.djvm.ComponentBuilder
import java.util.function.Function

class Serializer(private val classLoader: SandboxClassLoader) {
    private val factory: SerializationFactory
    private val context: SerializationContext

    init {
        val env = createSandboxSerializationEnv(classLoader)
        factory = env.serializationFactory
        context = env.p2pContext
    }

    /**
     * Generate a [Function] that deserializes a [ByteArray] into an instance
     * of the given sandbox class. We import this [Function] into the sandbox
     * so that [ComponentBuilder] can deserialize objects lazily.
     */
    fun deserializerFor(clazz: Class<*>): Function<ByteArray?, out Any?> {
        val sandboxClass = classLoader.toSandboxClass(clazz)
        return Function { bytes ->
            bytes?.run {
                ByteSequence.of(this).deserializeTo(sandboxClass, classLoader)
            }
        }
    }

    fun deserializeTo(clazz: Class<*>, bytes: ByteSequence): Any {
        val sandboxClass = classLoader.toSandboxClass(clazz)
        return bytes.deserializeTo(sandboxClass, classLoader, factory, context)
    }

    inline fun <reified T : Any> deserialize(bytes: SerializedBytes<T>?): Any? {
        return deserializeTo(T::class.java, bytes ?: return null)
    }
}
