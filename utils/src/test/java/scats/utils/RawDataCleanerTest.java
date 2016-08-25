package scats.utils;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yikai Gong on 25/08/16.
 */
public class RawDataCleanerTest extends TestCase {

    /**
     * Test the utils function for cleaning each line of raw data
     * by comparing output to expectation
     *
     * @throws Exception
     */
    public void testCleanScatsTupleStr() throws Exception {
        File rawFile = new File(this.getClass()
                .getResource("/raw_data/scats_vol.csv").getPath());
        File expFile = new File(this.getClass()
                .getResource("/expected_data/scats_vol.csv").getPath());
        BufferedReader rawReader = new BufferedReader(new FileReader(rawFile));
        BufferedReader expReader = new BufferedReader(new FileReader(expFile));
        List<String> cleanedLines = new ArrayList<>();
        List<String> expLines = new ArrayList<>();

        String expLine;
        while ((expLine = expReader.readLine()) != null) {
            expLines.add(expLine);
        }

        // Skip header
        rawReader.readLine();
        String rawLine;
        while ((rawLine = rawReader.readLine()) != null) {
            String result = RawVolumeDataCleaner.cleanScatsTupleStr(rawLine);
            if (result != null)
                cleanedLines.add(result);
        }

        assertEquals("Size of cleaned data does NOT match expected data",
                cleanedLines.size(), expLines.size());
        for (int i = 0; i < cleanedLines.size(); i++) {
            assertEquals("Content of cleaned data does NOT match expected data",
                    cleanedLines.get(i), expLines.get(i));
        }
    }

    public void testRemoveQuotation() throws Exception {

    }

}