import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Arrays;

public class DeserializeTest {

    @Test
    public void test_get_records_from_block() {
        try {
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            List<Object[]> result = calciteConnection.get_records_from_block("actor", 2);

            List<Integer> expected = Arrays.asList(
                    78, 79, 80, 81, 82, 83, 84, 85, 143, 86, 87, 88, 89, 90, 91, 92, 93,
                    94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
                    115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134,
                    135, 136, 137, 138, 139, 140, 141, 142, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155);

            // Uncomment this to test the function after implementing it

            assert(result.size() == 78);

            for(int i = 0; i < result.size(); i++){
                assert(result.get(i).length == 4);
                assert(result.get(i)[0] instanceof Integer);
                assert(result.get(i)[0].equals(expected.get(i)));
            }

            calciteConnection.close();

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }

        System.out.println("Test passed :)");
    }
}