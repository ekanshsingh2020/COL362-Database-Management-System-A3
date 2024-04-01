import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
public class BPlusTreeTest {

    @Test
    public void test_bplustree() {
        try {
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            calciteConnection.create_index("category", "category_id", 3);
            calciteConnection.return_bfs_index("category", "category_id");
            
            ArrayList<Integer> result = calciteConnection.return_bfs_index("category", "category_id");
            
            ArrayList<Integer> expected_result = new ArrayList<Integer>();
            expected_result.add(5);
            expected_result.add(9);
            expected_result.add(3);
            expected_result.add(7);
            expected_result.add(11);
            expected_result.add(13);
            expected_result.add(2);
            expected_result.add(4);
            expected_result.add(6);
            expected_result.add(8);
            expected_result.add(10);
            expected_result.add(12);
            expected_result.add(14);
            expected_result.add(15);
            for(int i = 1 ; i <= 16 ; i ++) {
                expected_result.add(i);
            }
            // 5 9 3 7 11 13 2 4 6 8 10 12 14 15 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16

            // Uncomment this to test the function after implementing it
            assert(result.size() == expected_result.size());
            for(int i = 0; i < result.size(); i++){
                assert(result.get(i) == expected_result.get(i));
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