package me.jaksa.limbo;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores a limited number of elements in a file. The operations are written sequentially in the file.
 * Once 100 insertions are performed only deletions and updtes are allowed. once every element is deleted
 * the file is deleted.
 */
class Batch<K, V> {
    static final int MAX_INSERTIONS = 100;

    private final Map<K, V> cache;
    private int elementsInserted = 0; // how many elements have been inserted in this Batch (regardless whether they're still there)
    private final ObjectOutputStream out;
    private final File file;


    Batch(File file) throws IOException, ClassNotFoundException {
        cache = (file.exists()) ? read(file) : new HashMap<>();
        out = new ObjectOutputStream(new FileOutputStream(file, true));
        this.file = file;
    }

    private Map<K, V> read(File file) throws IOException, ClassNotFoundException {
        HashMap<K, V> map = new HashMap<>();

        // FIS needs to be a separate resource if the file is empty, otherwise new OIS() throws EOFException
        try (FileInputStream fis = new FileInputStream(file); ObjectInputStream in = new ObjectInputStream(fis)) {
            while (true) {
                Object entry = in.readObject();
                if (entry == null) break;
                if (entry instanceof Batch.Save) {
                    Save<K, V> save = (Save) entry;
                    if (map.put(save.k, save.v) == null)
                        elementsInserted++;
                } else if (entry instanceof Batch.Delete) {
                    map.remove(((Delete) entry).k);
                }
            }
        } catch (EOFException e) {
            // fine
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    String getFileName() {
        return file.getName();
    }

    public void save(K k, V v) throws IOException {
        out.writeObject(new Save<>(k, v));
        out.flush();
        if (!cache.containsKey(k)) elementsInserted++;
        cache.put(k, v);
    }

    public void delete(K k) throws IOException {
        out.writeObject(new Delete<>(k));
        out.flush();
        cache.remove(k);
        if (cache.isEmpty()) {
            out.close();
            System.out.println(file.delete());
        }
    }

    public Map<K, V> getAll() { return Collections.unmodifiableMap(cache); }

    public boolean shouldInsertMore() { return elementsInserted < MAX_INSERTIONS; }

    public boolean readyForGC() { return !shouldInsertMore() && cache.size() == 0; }

    public void close() throws IOException {
        out.close();
    }

    private static class Save<K, V> implements Serializable {
        final K k;
        final V v;

        Save(K k, V v) { this.k = k; this.v = v; }
    }

    private static class Delete<K> implements Serializable {
        final K k;

        Delete(K k) { this.k = k; }
    }
}
