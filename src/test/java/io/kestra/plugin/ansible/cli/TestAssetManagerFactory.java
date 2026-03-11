package io.kestra.plugin.ansible.cli;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.AssetEmitter;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

@Singleton
@Replaces(AssetManagerFactory.class)
public class TestAssetManagerFactory extends AssetManagerFactory {
    private final TestAssetEmitter emitter = new TestAssetEmitter();

    @Override
    public AssetEmitter of(boolean enabled) {
        emitter.setEnabled(enabled);
        return emitter;
    }

    public TestAssetEmitter emitter() {
        return emitter;
    }

    public void reset() {
        emitter.reset();
    }

    static class TestAssetEmitter implements AssetEmitter {
        private final List<AssetEmit> emitted = new CopyOnWriteArrayList<>();
        private volatile boolean enabled;

        void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        void reset() {
            emitted.clear();
        }

        @Override
        public void emit(AssetEmit assetEmit) {
            if (!enabled) {
                return;
            }
            emitted.add(assetEmit);
        }

        @Override
        public List<AssetEmit> emitted() {
            return List.copyOf(emitted);
        }
    }
}
