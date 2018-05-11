package me.jaksa.limbo;

import org.junit.Test;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class BatchTest {

    @Test
    public void testInsertingObjects() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        batch.save(1, "One");
        batch.save(2, "Two");
        batch.save(3, "Three");

        assertThat(batch.getAll().get(1), is("One"));
        assertThat(batch.getAll().get(2), is("Two"));
        assertThat(batch.getAll().get(3), is("Three"));
        batch.close();

        batch = new Batch(file);
        assertThat(batch.getAll().get(1), is("One"));
        assertThat(batch.getAll().get(2), is("Two"));
        assertThat(batch.getAll().get(3), is("Three"));
    }

    @Test
    public void testDeletingObjects() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        batch.save(1, "One");
        batch.save(2, "Two");
        batch.save(3, "Three");
        batch.delete(2);

        assertThat(batch.getAll().get(1), is("One"));
        assertThat(batch.getAll().get(2), is(nullValue()));
        assertThat(batch.getAll().get(3), is("Three"));
        batch.close();

        batch = new Batch(file);
        assertThat(batch.getAll().get(1), is("One"));
        assertThat(batch.getAll().get(2), is(nullValue()));
        assertThat(batch.getAll().get(3), is("Three"));
    }

    @Test
    public void testOverwritingObjects() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        batch.save(1, "One");
        batch.save(2, "Two");
        batch.save(3, "Three");
        batch.save(2, "Zwei");

        assertThat(batch.getAll().get(1), is("One"));
        assertThat(batch.getAll().get(2), is("Zwei"));
        assertThat(batch.getAll().get(3), is("Three"));
        batch.close();

        batch = new Batch(file);
        assertThat(batch.getAll().get(1), is("One"));
        assertThat(batch.getAll().get(2), is("Zwei"));
        assertThat(batch.getAll().get(3), is("Three"));
    }

    @Test
    public void testMaxInserts() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        for (int i = 1; i <= Batch.MAX_INSERTIONS; i++) batch.save(i, "" + i); // insert 100 objects

        assertFalse(batch.shouldInsertMore());
        batch.close();

        batch = new Batch(file);
        assertFalse(batch.shouldInsertMore());
    }

    @Test
    public void testMaxInserts2() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        for (int i = 1; i <= Batch.MAX_INSERTIONS - 1; i++) batch.save(i, "" + i); // insert 100 objects

        assertTrue(batch.shouldInsertMore());
        batch.close();

        batch = new Batch(file); // simulate restart
        assertTrue(batch.shouldInsertMore());
    }

    @Test
    public void testMaxInsertsWithupdates() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        for (int i = 1; i <= Batch.MAX_INSERTIONS - 1; i++) {
            batch.save(i, "new" + i);
            batch.save(i, "updated" + i);
        }

        assertTrue(batch.shouldInsertMore());
        batch.close();

        batch = new Batch(file); // simulate restart
        assertTrue(batch.shouldInsertMore());
    }

    @Test
    public void testDeletionOfFile() throws Exception {
        File file = File.createTempFile("batch", ".lb");
        Batch batch = new Batch(file);
        for (int i = 1; i <= Batch.MAX_INSERTIONS; i++) batch.save(i, "" + i);
        for (int i = 1; i <= Batch.MAX_INSERTIONS; i++) batch.delete(i);

        assertFalse(file.exists());
    }
}