package net.corda.nodeapi.internal.serialization;

import com.google.common.collect.Maps;
import net.corda.core.serialization.SerializationContext;
import net.corda.core.serialization.SerializationDefaults;
import net.corda.core.serialization.SerializationFactory;
import net.corda.core.serialization.SerializedBytes;
import net.corda.testing.TestDependencyInjectionBase;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;

public final class ForbiddenLambdaSerializationTests extends TestDependencyInjectionBase {

    private SerializationFactory factory;

    @Before
    public void setup() {
        factory = SerializationDefaults.INSTANCE.getSERIALIZATION_FACTORY();
    }

    @Test
    public final void serialization_fails_for_serializable_java_lambdas() throws Exception {
        EnumSet<SerializationContext.UseCase> contexts = EnumSet.complementOf(EnumSet.of(SerializationContext.UseCase.Checkpoint));

        contexts.forEach(ctx -> {
            SerializationContext context = new SerializationContextImpl(SerializationSchemeKt.getKryoHeaderV0_1(), this.getClass().getClassLoader(), AllWhitelist.INSTANCE, Maps.newHashMap(), true, ctx);

            String value = "Hey";
            Callable<String> target = (Callable<String> & Serializable) () -> value;

            Throwable throwable = catchThrowable(() -> serialize(target, context));

            assertThat(throwable).isNotNull();
            assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
            if (ctx != SerializationContext.UseCase.RPCServer && ctx != SerializationContext.UseCase.Storage) {
                assertThat(throwable).hasMessage(CordaClosureBlacklistSerializer.INSTANCE.getERROR_MESSAGE());
            } else {
                assertThat(throwable).hasMessageContaining("RPC not allowed to deserialise internal classes");
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public final void serialization_fails_for_not_serializable_java_lambdas() throws Exception {
        EnumSet<SerializationContext.UseCase> contexts = EnumSet.complementOf(EnumSet.of(SerializationContext.UseCase.Checkpoint));

        contexts.forEach(ctx -> {
            SerializationContext context = new SerializationContextImpl(SerializationSchemeKt.getKryoHeaderV0_1(), this.getClass().getClassLoader(), AllWhitelist.INSTANCE, Maps.newHashMap(), true, ctx);

            String value = "Hey";
            Callable<String> target = () -> value;

            Throwable throwable = catchThrowable(() -> serialize(target, context));

            assertThat(throwable).isNotNull();
            assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
            assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
            if (ctx != SerializationContext.UseCase.RPCServer && ctx != SerializationContext.UseCase.Storage) {
                assertThat(throwable).hasMessage(CordaClosureBlacklistSerializer.INSTANCE.getERROR_MESSAGE());
            } else {
                assertThat(throwable).hasMessageContaining("RPC not allowed to deserialise internal classes");
            }
        });
    }

    private <T> SerializedBytes<T> serialize(final T target, final SerializationContext context) {
        return factory.serialize(target, context);
    }
}