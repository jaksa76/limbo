package me.jaksa.limbo;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

/**
 * Persistent key-value store. Operations are fast as long as there are no long lived objects. The more long lived
 * objects there are. The longer it takes to read/update/delete. Insertions of new elements are always fast.
 */
public class Limbo<K, V> {
    private final File dir;
    private Set<Batch> batches = new HashSet<>();
    private Batch currentBatch;

    public Limbo(File dir) throws IOException, ClassNotFoundException {
        if (!dir.exists()) dir.mkdirs();
        File[] files = dir.listFiles(f -> f.getName().matches("\\d+\\.lb"));

        for (File file : files) {
            Batch<K, V> batch = new Batch<>(file);
            batches.add(batch);
            if (currentBatch == null || getBatchNumber(batch) > getBatchNumber(currentBatch)) currentBatch = batch;
        }
        this.dir = dir;
    }

    public void insert(K k, V v) throws IOException {
        if (currentBatch == null || !currentBatch.shouldInsertMore()) {
            currentBatch = createNewBatch();
            batches.add(currentBatch);
        }
        currentBatch.save(k, v);
    }

    private Batch createNewBatch() throws IOException {
        try {
            int batchNum = batches.stream().map(this::getBatchNumber).max(Integer::compareTo).map(n -> n + 1).orElse(1);
            return new Batch(new File(dir, batchNum + ".lb"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Batch file should not exist", e);
        }
    }

    private int getBatchNumber(Batch batch) {
        return Integer.parseInt(batch.getFileName().replace(".lb", ""));
    }

    public void update(K k, V v) throws IOException {
        Batch batch = findBatchFor(k);
        if (batch == null) throw new IllegalStateException("Could not find element with key " + k);
        batch.save(k, v);
    }

    public void remove(K k) throws IOException {
        Batch batch = findBatchFor(k);
        if (batch == null) throw new IllegalStateException("Could not find element with key " + k);
        batch.delete(k);
        if (batch.readyForGC()) batches.remove(batch);
    }

    private Batch findBatchFor(K k) {
        for (Batch batch : batches) {
            if (batch.getAll().containsKey(k)) return batch;
        }
        return null;
    }

    public Map<K, V> getAll() {
        HashMap<K, V> map = new HashMap<>();
        for (Batch batch : batches) map.putAll(batch.getAll());
        return map;
    }

    public void close() {
        for (Batch batch : batches) {
            try {
                batch.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
