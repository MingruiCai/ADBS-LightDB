package ed.inf.adbs.lightdb;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Scanner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestGivenExample {
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

    public boolean compare(String num) {
        String[] args = new String[]{"samples/db", "samples/input/query" + num + ".sql", "samples/output/query" + num + ".csv"};
        LightDB.main(args);
        String expected = "";
        try {
            File file = new File("samples/expected_output/query" + num + ".csv");
            Scanner scanner = new Scanner(file);
            while (scanner.hasNext()) {
                expected = expected + scanner.next() + "\n";
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail();
        }
        String my = "";
        try {
            File file = new File("samples/output/query" + num + ".csv");
            Scanner scanner = new Scanner(file);
            while (scanner.hasNext()) {
                my = my + scanner.next() + "\n";
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail();
        }
        for (int i = 0; i < my.length(); i++) {
            if (i == expected.length() || my.charAt(i) != expected.charAt(i)) {
                return false;
            }
        }
        File myObj = new File("samples/output/query" + num + ".csv");
        if (myObj.delete()) {
            System.out.println("Pass example NO. "+ num );
        } else {
            System.out.println("Failed to delete the file.");
            fail();
        }
        return true;
    }

    @Test
    public void testMain1() throws IOException {
        assertTrue(compare("1"));
    }

    @Test
    public void testMain2() throws IOException {
        assertTrue(compare("2"));
    }

    @Test
    public void testMain3() throws IOException {
        assertTrue(compare("3"));
    }

    @Test
    public void testMain4() throws IOException {
        assertTrue(compare("4"));
    }

    @Test
    public void testMain5() throws IOException {
        assertTrue(compare("5"));
    }

    @Test
    public void testMain6() throws IOException {
        assertTrue(compare("6"));
    }

    @Test
    public void testMain7() throws IOException {
        assertTrue(compare("7"));
    }

    @Test
    public void testMain8() throws IOException {
        assertTrue(compare("8"));
    }

}
