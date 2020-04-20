/*
 * Copyright(C) 2019, 2020. See COPYING for more.
 */
package org.sdf.etoile;

import java.io.File;
import java.util.List;

/**
 * Handles files for test cases.
 * @since 0.5.0
 */
public interface TestFiles {
    /**
     * Write data to input file with directory.
     * @param dir Parent directory.
     * @param lines Input lines.
     */
    void writeInputWithDirectory(String dir, String... lines);

    /**
     * Write data to input file.
     * @param lines Lines.
     */
    void writeInput(String... lines);

    /**
     * Read lines from output.
     * @return List of lines.
     */
    List<String> outputLines();

    /**
     * Resolves output.
     * @return Output directory.
     */
    File output();

    /**
     * Copy resource to input.
     * @param name Resource name.
     */
    void copyResource(String name);

    /**
     * List files in output directory.
     * @return List of files.
     */
    List<File> outputFiles();

    /**
     * Resolves input.
     * @return Input directory.
     */
    File input();
}
