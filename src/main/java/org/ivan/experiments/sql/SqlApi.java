package org.ivan.experiments.sql;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SqlApi {
    private static final Convention IVAN_CONVENTION = new Convention.Impl("Ivan", IvanRel.class);

    public interface IvanRel extends RelNode {
    }

    public static class IvanTableScan extends TableScan implements IvanRel {
        public IvanTableScan(RelOptCluster cluster, RelOptTable table) {
            super(cluster, RelTraitSet.createEmpty().plus(IVAN_CONVENTION).plus(RelDistributions.ANY), table);
        }
    }

    public static class IvanProject extends Project implements IvanRel {
        protected IvanProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
            super(cluster, traits, input, projects, rowType);
        }

        @Override
        public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
            return this;
        }
    }

    private final ConcurrentHashMap<String, SqlTable> tables = new ConcurrentHashMap<>();

    private final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    public void setupTable(String name, Iterable<SqlTuple> tuples) {
        SqlTuple first = tuples.iterator().next();
        assert first != null;

        tables.put(name.toUpperCase(), tuples::iterator);

        class MyTable extends AbstractTable implements
//                TranslatableTable
                ScannableTable
        {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                // WARN: record type should be cached
                ArrayList<RelDataTypeField> fields = new ArrayList<>();
                int i = 0;
                for (String cn : first.columnNames()) {
                    fields.add(new RelDataTypeFieldImpl(
                            cn.toUpperCase(), i++, typeFactory.createJavaType(first.column(cn).getClass())));
                }
                return new RelRecordType(fields);
            }

//            @Override
            public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
                return new IvanTableScan(context.getCluster(), relOptTable);
            }

//            @Override
            public Enumerable<Object[]> scan(DataContext root) {
                return Linq4j.asEnumerable(() -> {
                    Iterator<SqlTuple> it = tables.get(name.toUpperCase()).iterator();
                    return new Iterator<Object[]>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Object[] next() {
                            SqlTuple tuple = it.next();
                            Object[] objs = new Object[tuple.columnNames().size()];
                            for (int i = 0; i < objs.length; i++) {
                                objs[i] = tuple.column(i);
                            }
                            return objs;
                        }
                    };
                });
            }
        }

        // todo synchronization
        rootSchema.add(name.toUpperCase(), new MyTable());
    }

    public List<List<Object>> query(String sql) throws Exception {
//        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.emptyList(),
                typeFactory,
                null);

        // todo check why validator is mandatory
        SqlValidator validator = new SimpleSqlValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlConformanceEnum.DEFAULT);

        SqlNode parseTree = validator.validate(
                SqlParser.create(sql, SqlParser.configBuilder().build()).parseQuery());

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setNoneConventionHasInfiniteCost(false);

        class Rule1 extends RelOptRule {
            public Rule1() {
                super(RelOptRule.operand(LogicalProject.class, Convention.NONE, RelOptRule.any()), "Rule1");
            }

            @Override
            public void onMatch(RelOptRuleCall call) {
                LogicalProject project = ((LogicalProject) call.rel(0));

//                RelNode convertedInput = RelOptRule.convert(project.getInput(), project.getInput().getTraitSet().plus(IVAN_CONVENTION));
                RelNode convertedInput = project.getInput();

                call.transformTo(new IvanProject(project.getCluster(), project.getTraitSet().plus(IVAN_CONVENTION), convertedInput, project.getProjects(), project.getRowType()));
            }
        }

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
//        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

        RelRoot relTree = sqlToRelConverter(typeFactory, validator, catalogReader, planner)
                .convertQuery(parseTree, false, true);

        RelNode rel0 = relTree.rel;

        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder()
                .addRuleInstance(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE)
                .addRuleInstance(EnumerableRules.ENUMERABLE_FILTER_RULE)
                .addRuleInstance(EnumerableRules.ENUMERABLE_CALC_RULE)
                .addRuleInstance(EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE)
                .addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_RULE)
                .addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE)
                .build());

        hepPlanner.setRoot(rel0);
        rel0 = hepPlanner.findBestExp();

        Program program = Programs.ofRules(
//                new Rule1(),
                // needed to enable traits conversion based on trait def
                AbstractConverter.ExpandConversionRule.INSTANCE);
        planner.setRoot(rel0);

        rel0 = program.run(
                planner,
                rel0,
                RelTraitSet.createEmpty()
//                        .plus(Convention.NONE)
                        .plus(EnumerableConvention.INSTANCE)
//                        .plus(RelDistributions.SINGLETON)
                ,
                Collections.emptyList(),
                Collections.emptyList());

//        return executablePlan(relTree).collect(Collectors.toList());
        return enumerablePlan(rel0);
    }

    private SqlToRelConverter sqlToRelConverter(
            RelDataTypeFactory typeFactory, SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptPlanner planner) {
        RelOptCluster relOptCluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        return new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                relOptCluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.configBuilder()
                        // to have a logical scan for a table access, todo check better alternatives
//                        .withConvertTableAccess(false)
                        .build());
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> enumerablePlan(RelNode rel) {
        Enumerable<Object[]> e = EnumerableInterpretable.toBindable(Collections.emptyMap(), null, (EnumerableRel)rel, EnumerableRel.Prefer.ARRAY)
                .bind(new StubDataContext());
        return StreamSupport.stream(e.spliterator(), false)
                .map(Arrays::asList)
                .collect(Collectors.toList());
    }

    public class StubDataContext implements DataContext {
        @Override
        public SchemaPlus getRootSchema() {
            return rootSchema.plus();
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return (JavaTypeFactory) typeFactory;
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(String name) {
            return null;
        }
    }

    private Stream<List<Object>> executablePlan(RelRoot relRoot) {
        SimpleRelShuttle sh = new SimpleRelShuttle();

        relRoot.rel.accept(sh);

        return sh.executablePlan();
    }

    private static class SimpleSqlValidator extends SqlValidatorImpl {
        private SimpleSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
                                   RelDataTypeFactory typeFactory, SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, conformance);
        }
    }

    private class SimpleRelShuttle extends RelShuttleImpl {
        private Stream<Object> sourceStream;
        private Deque<Function<Stream<Object>, Stream<Object>>> transformers = new LinkedList<>();

        private Stream<List<Object>> executablePlan() {
            Stream<Object> res = sourceStream;
            for (Function<Stream<Object>, Stream<Object>> t : transformers) {
                res = t.apply(res);
            }
            return res.map(x -> (List<Object>) x);
        }

        @Override
        public RelNode visit(TableScan scan) {
            // todo realize qualified name structure
            String name = scan.getTable().getQualifiedName().get(0);

            // todo build source stream
            sourceStream = tables.get(name).stream().map(Function.identity());

            return super.visit(scan);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            transformers.addFirst(stream -> stream.map(x -> {
                SqlTuple tup = (SqlTuple) x;
                return project.getRowType().getFieldNames().stream()
                        .map(tup::column)
                        .collect(Collectors.toList());
            }));

            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RexNode condition = filter.getCondition();

            transformers.addFirst(stream -> stream.filter(x -> {
                SqlTuple tup = (SqlTuple) x;

                ConditionEvaluator condEval = new ConditionEvaluator(tup);
                condition.accept(condEval);

                return condEval.isTrue();
            }));

            return super.visit(filter);
        }

        // todo visit root node?
    }

    private static class ConditionEvaluator extends RexVisitorImpl {
        private final SqlTuple tup;
        private Object lhs;
        private RexLiteral rhs;

        protected ConditionEvaluator(SqlTuple tup) {
            super(true);
            this.tup = tup;
        }

        @Override
        public Object visitCall(RexCall call) {
            SqlOperator op = call.getOperator();
            if (op.getKind() == SqlKind.EQUALS) {
                // dirty-dirty
                rhs = (RexLiteral) call.getOperands().get(1);
            }
            else if (op instanceof SqlCastFunction) {
                // continue
            }
            else {
                assert false;
            }

            return super.visitCall(call);
        }

        @Override
        public Object visitInputRef(RexInputRef inputRef) {
            // todo figure out how to access input references properly, by index, by name, etc.
            // dirty-dirty
            lhs = tup.column(inputRef.getIndex());

            return super.visitInputRef(inputRef);
        }

        private boolean isTrue() {
            assert lhs != null && rhs != null;

            return lhs.equals(rhs.getValueAs(lhs.getClass()));
        }
    }
}
