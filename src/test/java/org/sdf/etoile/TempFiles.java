/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.io.IOUtil;

/**
 * Temporary files for tests.
 * @since 0.5.0
 */
public final class TempFiles implements TestFiles {

    /**
     * Temporary folder.
     */
    private final TemporaryFolder temp;

    /**
     * Cache of created/resolved directories.
     */
    private final Map<String, File> cache;

    /**
     * Ctor.
     * @param temp Temporary folder.
     */
    public TempFiles(final TemporaryFolder temp) {
        this.temp = temp;
        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public void writeInputWithDirectory(final String dir, final String... lines) {
        final Path parent = this.input().toPath().resolve(dir);
        parent.toFile().mkdirs();
        IOUtil.writeText(
            String.join(System.lineSeparator(), lines),
            parent
                .resolve(String.format("part-%s.csv", UUID.randomUUID()))
                .toFile()
        );
    }

    @Override
    public void writeInput(final String... lines) {
        this.writeInputWithDirectory(".", lines);
    }

    @Override
    public List<String> outputLines() {
        final List<String> lines = new ArrayList<>(100);
        for (final File csv : this.outputFiles()) {
            try {
                lines.addAll(IOUtils.readLines(Files.newInputStream(csv.toPath())));
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        return Collections.unmodifiableList(lines);
    }

    @Override
    public File output() {
        return this.resolve("output");
    }

    @Override
    public void copyResource(final String name) {
        final File copy = Paths.get(this.input().getAbsolutePath(), name).toFile();
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream stream = loader.getResourceAsStream(name)) {
            try (OutputStream output = Files.newOutputStream(copy.toPath())) {
                IOUtils.copy(Objects.requireNonNull(stream), output);
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public List<File> outputFiles() {
        return Arrays.stream(
            Objects.requireNonNull(
                this.output()
                    .listFiles((dir, name) -> name.endsWith(".csv"))
            )
        ).collect(Collectors.toList());
    }

    @Override
    public File input() {
        final File input = this.resolve("input");
        input.mkdirs();
        return input;
    }

    /**
     * Resolves non-existent directory and fills cache.
     * @param dir Directory name.
     * @return Directory.
     */
    private File resolve(final String dir) {
        return this.cache.computeIfAbsent(
            dir,
            key -> {
                try {
                    return this.temp.newFolder()
                        .toPath()
                        .resolve(dir)
                        .toFile();
                } catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        );
    }
}
