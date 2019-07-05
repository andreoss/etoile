package org.sdf.etoile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.cactoos.Func;
import org.cactoos.collection.Filtered;
import org.cactoos.collection.Mapped;
import org.cactoos.list.ListOf;
import org.cactoos.text.JoinedText;
import org.cactoos.text.TextEnvelope;
import org.cactoos.text.TextOf;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.llorllale.cactoos.matchers.TextIs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

final class ConcatenatedText extends TextEnvelope {
    ConcatenatedText(final Path directory, final String extenstion) {
        this(directory, (File f) -> f.getName()
                .endsWith(extenstion));
    }

    ConcatenatedText(final Path directory, final Func<File, Boolean> filter) {
        super(
                new JoinedText(
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

public final class HeaderCsvOutputTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private SparkSession session;

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .master("local[1]")
                .getOrCreate();
    }

    @Test
    public void addsHeaderForEmptyOutput() throws IOException {
        final Path output = temp.newFolder("output")
                .toPath()
                .resolve("csv");
        new HeaderCsvOutput<>(
                new FakeInput(session, "id int, name string"),
                output
        ).run();
        MatcherAssert.assertThat(
                "writes csv with header",
                new ConcatenatedText(output, "csv"),
                new TextIs(
                        "id,name\n"
                )
        );
    }

    @Test
    public void addsHeader() throws IOException {
        final Path output = temp.newFolder("output")
                .toPath()
                .resolve("csv");
        final Row o = new GenericRowWithSchema(
                new Object[]{1, "foo"},
                StructType.fromDDL("id int, name string")
        );
        new HeaderCsvOutput<>(
                new FakeInput(
                        session,
                        "id int, name string",
                        Arrays.asList(
                                Factory.arrayOf(1, "foo"),
                                Factory.arrayOf(2, "bar")
                        )
                ),
                output
        ).run();
        MatcherAssert.assertThat(
                "writes csv with header",
                new ConcatenatedText(output, "csv"),
                new LinesAre(
                        "id,name",
                        "1,foo",
                        "2,bar\n"
                )
        );
    }

}
