import optimizer.convention.PConvention;
import optimizer.rules.PRules;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class MyCalciteConnectionTest {

    @Test 
    public void testSFW() {

        try{

            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            
            System.out.println("\nCreating index on actor_id...");
            calciteConnection.create_index("actor", "actor_id", 10);

            SqlNode sqlNode = calciteConnection.parseSql("select * from actor\n"
                                    + "where actor_id >= 100");
                                    
            System.out.println("\n[+] Parsed SQL: \n" + sqlNode);
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("\n[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("\n[+] Converted SQL: \n" + relNode);

            RuleSet rules = RuleSets.ofList(
                PRules.PCustomRule.INSTANCE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("\n[+] Physical SQL: \n" + phyRelNode);

            System.out.println("\n[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.evaluate(phyRelNode);

            // Uncomment this to check the records returned by IndexScan
            
            assert (result.size() == 101);

            List <Integer> actor_ids = new ArrayList<>();

            for (Object [] row : result) {
                assert (row.length == 4);
                assert (row[0] instanceof Integer);
                assert ((Integer)row[0] >= 100);
                actor_ids.add((Integer)row[0]);
            }

            // sort the actor_ids
            actor_ids.sort(null);

            // result actor_ids should be from 100 to 200
            for (int i = 0; i < actor_ids.size(); i++) {
                assert (actor_ids.get(i).equals(100 + i));
            }

            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("\nTest passed :)\n");
        return;
    }
}
