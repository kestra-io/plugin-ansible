package io.kestra.plugin.ansible.cli;

import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.runners.AssetEmitter;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
        private final List<Asset> upserts = new CopyOnWriteArrayList<>();
        private volatile boolean enabled;

        void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        void reset() {
            upserts.clear();
        }

        List<Asset> upserts() {
            return List.copyOf(upserts);
        }

        @Override
        public void upsert(Asset asset) {
            if (!enabled) {
                return;
            }
            upserts.add(asset);
        }

        @Override
        public List<Asset> outputs() {
            return List.of();
        }
    }
}
