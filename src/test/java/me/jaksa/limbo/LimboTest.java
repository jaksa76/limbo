package me.jaksa.limbo;

import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class LimboTest {
    @Test
    public void testInsertingMoreThan100Elements() throws Exception {
        File dir = Files.createTempDirectory("limbo").toFile();
        Limbo<Integer, String> limbo = new Limbo<>(dir);

        for (int i = 0; i < 120; i++) {
            limbo.insert(i, "" + i);
        }
    }
}