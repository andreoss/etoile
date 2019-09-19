package org.sdf.etoile;

import org.cactoos.Func;
import org.cactoos.collection.Filtered;
import org.cactoos.collection.Mapped;
import org.cactoos.list.ListOf;
import org.cactoos.text.Joined;
import org.cactoos.text.TextEnvelope;
import org.cactoos.text.TextOf;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.llorllale.cactoos.matchers.TextIs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;


final class CsvText extends TextEnvelope {
    private CsvText(final Path input) {
        super(
                new ConcatenatedText(input, "csv")
        );
    }

    CsvText(final Terminal input) {
        this(input.result());
    }

    CsvText(final File output) {
        this(output.toPath());
    }

    private CsvText(final URI uri) {
        this(Paths.get(uri));
    }
}

final class ConcatenatedText extends TextEnvelope {
    ConcatenatedText(final Path directory, final String extension) {
        this(directory, (File f) -> f.getName()
                .endsWith(extension));
    }

    private ConcatenatedText(final Path directory, final Func<File, Boolean> filter) {
        super(
                new Joined(
                        new TextOf(""),
                        new Mapped<>(
                                TextOf::new,
                                new Filtered<>(
                                        filter,
                                        new ListOf<>(
                                                Objects.requireNonNull(
                                                        directory.toFile()
                                                                .listFiles(File::isFile)
                                                )
                                        )
                                )
                        )
                )
        );
    }
}

final class HeaderCsvOutputTest extends SparkTestTemplate {
    @Test
    void addsHeaderForEmptyOutput() throws IOException {
        final Path output = temp.newFolder("output")
                .toPath()
                .resolve("csv");
        MatcherAssert.assertThat(
                "writes csv with header",
                new CsvText(
                        new Saved<>(
                                output.toUri(),
                                new HeaderCsvOutput<>(
                                        new FakeInput(session, "id int, name string")
                                )
                        )
                ),
                new TextIs(
                        "id,name" + System.lineSeparator()
                )
        );
    }

    @Test
    void addsHeader() throws IOException {
        final Path output = temp.newFolder("output")
                .toPath()
                .resolve("csv");
        MatcherAssert.assertThat(
                "writes csv with header",
                new CsvText(
                        new Saved<>(
                                output,
                                new HeaderCsvOutput<>(
                                        new FakeInput(
                                                session,
                                                "id int, name string",
                                                Arrays.asList(
                                                        Factory.arrayOf(1, "foo"),
                                                        Factory.arrayOf(2, "bar")
                                                )
                                        )
                                )
                        )
                ),
                new LinesAre(
                        "id,name",
                        "1,foo",
                        "2,bar"
                )
        );
    }

    @Test
    void checksParametersForHeaderOptions() throws IOException {
        Assertions.assertThrows(IllegalArgumentException.class,
                new HeaderCsvOutput<>(
                        new FakeInput(session, "id int"),
                        Collections.singletonMap("header", "true")
                )::get
        );
    }
}
