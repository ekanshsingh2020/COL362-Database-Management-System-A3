package optimizer.rules;

import org.apache.calcite.adapter.file.CsvTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;

import optimizer.convention.PConvention;
import optimizer.rel.PIndexScan;

public class PRules {

    private PRules(){
    }

    // Matches Project over Filter over TableScan, extends RelOptRule
    public static class PCustomRule extends RelOptRule {

        public static final PCustomRule INSTANCE = new PCustomRule();

        public PCustomRule() {
            super(operand(LogicalProject.class, operand(LogicalFilter.class, operand(CsvTableScan.class, none()))));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalProject project = call.rel(0);
            final LogicalFilter filter = call.rel(1);
            final CsvTableScan scan = call.rel(2);
            call.transformTo(new PIndexScan(scan.getCluster(), scan.getTraitSet().replace(PConvention.INSTANCE), scan.getTable(), filter.getCondition(), project.getProjects()));
        }

    }

}