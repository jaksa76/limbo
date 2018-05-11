package me.jaksa.limbo;

import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class LimboTest {
    @Test
    public void testInsertingMoreThan100Elements() throws Exception {
        File dir = Files.createTempDirectory("limbo").toFile();
        Limbo<Integer, String> limbo = new Limbo<>(dir);

        for (int i = 0; i < 120; i++) limbo.insert(i, "" + i);
        for (int i = 0; i < 120; i++) assertThat(limbo.getAll().get(i), is("" + i));

        limbo.close();

        limbo = new Limbo<>(dir);
        for (int i = 0; i < 120; i++) assertThat(limbo.getAll().get(i), is("" + i));
    }

    @Test
    public void testDeletingElements() throws Exception {
        File dir = Files.createTempDirectory("limbo").toFile();
        Limbo<Integer, String> limbo = new Limbo<>(dir);

        for (int i = 0; i < 120; i++) limbo.insert(i, "" + i);
        for (int i = 0; i < 120; i = i+2) limbo.remove(i); // remove even numbers
        for (int i = 0; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is(nullValue()));
        for (int i = 1; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is("" + i));
        limbo.close();

        limbo = new Limbo<>(dir);
        for (int i = 0; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is(nullValue()));
        for (int i = 1; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is("" + i));
    }

    @Test
    public void testUpdatingElements() throws Exception {
        File dir = Files.createTempDirectory("limbo").toFile();
        Limbo<Integer, String> limbo = new Limbo<>(dir);

        for (int i = 0; i < 120; i++) limbo.insert(i, "" + i);
        for (int i = 0; i < 120; i = i+2) limbo.update(i, "new" + i); // remove even numbers
        for (int i = 0; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is("new" + i));
        for (int i = 1; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is("" + i));
        limbo.close();

        limbo = new Limbo<>(dir);
        for (int i = 0; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is("new" + i));
        for (int i = 1; i < 120; i = i+2) assertThat(limbo.getAll().get(i), is("" + i));
    }

}
