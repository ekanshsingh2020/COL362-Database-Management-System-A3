import com.google.common.collect.ImmutableList;

import org.apache.calcite.util.Sources;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import optimizer.rel.PRel;
import manager.StorageManager;

import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.ArrayList;

public class MyCalciteConnection {

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        // print the absolute path of this class
        return Sources.of(MyCalciteConnection.class.getResource("/" + path)).file().getAbsolutePath();
    }

    private Connection connection;
    private SqlValidator validator;
    private SqlToRelConverter converter;
    private VolcanoPlanner planner;
    private StorageManager storage_manager;
    
    public MyCalciteConnection() throws Exception {

        storage_manager = new StorageManager();
        Properties info = new Properties();
        info.put("model", jsonPath("model"));
        info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());

        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            throw e;
        }

        CalciteConnection calciteConnection = (CalciteConnection) connection;
        Schema schema = calciteConnection.getRootSchema();

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getSchemas(null, null);

        rs.next();
        schema = schema.getSubSchema(rs.getString("TABLE_SCHEM"));

        CalciteSchema calciteSchema = CalciteSchema.from((SchemaPlus) schema);
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(info);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        ResultSet columns = metaData.getColumns(null, null, null, null);

        // hashmap from string table name to list of RelDataType
        HashMap<String, List<RelDataType>> tableSchema = new HashMap<>();

        while (columns.next()) {
            if (columns.getString("TABLE_NAME").equals("COLUMNS") || columns.getString("TABLE_NAME").equals("TABLES")) {
                continue;
            }
            String type = columns.getString("TYPE_NAME");
            if (type.endsWith(" NOT NULL")) {
                type = type.substring(0, type.length() - 9);
            }
            if (!tableSchema.containsKey(columns.getString("TABLE_NAME"))) {
                tableSchema.put(columns.getString("TABLE_NAME"), new ArrayList<>());
            }
            tableSchema.get(columns.getString("TABLE_NAME")).add((new RelDataTypeFieldImpl(
                columns.getString("COLUMN_NAME"),
                0,
                typeFactory.createSqlType(SqlTypeName.get(type))
            )).getType());
        }

        // load the tables
        for (String table : tableSchema.keySet()) {
            List<RelDataType> fields = tableSchema.get(table);
            storage_manager.loadFile(table + ".csv", fields);
        }
        System.out.println("Done loading files");

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            calciteSchema,
            Collections.singletonList(rs.getString("TABLE_SCHEM")),
            typeFactory,
            config
        );

        SqlOperatorTable operatorTable = new ChainedSqlOperatorTable(ImmutableList.of(SqlStdOperatorTable.instance()));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    public void close() throws Exception{
        connection.close();
    }

    public SqlNode parseSql(String sql) throws Exception{
        SqlParser parser = SqlParser.create(sql);
        return parser.parseStmt();
    }

    public SqlNode validateSql(SqlNode sqlNode) throws Exception{
        return validator.validate(sqlNode);
    }

    public RelNode convertSql(SqlNode sqlNode) throws Exception{
        return converter.convertQuery(sqlNode, false, true).rel;
    }

    public RelNode logicalToPhysical(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        Program program = Programs.of(RuleSets.ofList(rules));
        return program.run(
                planner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList()
        );
    }

    public void create_index(String table, String column_name, int order){
        storage_manager.create_index(table, column_name, order);
        return;
    }

    public List<Object []> evaluate(RelNode node) {
        return ((PRel) node).evaluate(storage_manager);
    }

    // will be used in tests
    public List<Object []> get_records_from_block(String table, int block_id) {
        return storage_manager.get_records_from_block(table, block_id);
    }

    public <T> ArrayList<T> return_bfs_index(String table, String column_name){
        return storage_manager.return_bfs_index(table, column_name);
    }

}