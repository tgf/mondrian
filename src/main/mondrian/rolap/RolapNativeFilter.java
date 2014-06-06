/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

/**
 * Computes a Filter(set, condition) in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeFilter extends RolapNativeSet {

    public RolapNativeFilter() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeFilter.get());
    }

    static class FilterConstraint extends SetConstraint {
        Exp filterExpr;
        boolean existing = false;
        public FilterConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            Exp filterExpr,
            boolean existing)
        {
            super(args, evaluator, true);
            this.filterExpr = filterExpr;
            this.existing = existing;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Overriding isJoinRequired() for native filters because
         * we have to force a join to the fact table if the filter
         * expression references a measure.
         */
        protected boolean isJoinRequired() {
            // Use a visitor and check all member expressions.
            // If any of them is a measure, we will have to
            // force the join to the fact table. If it is something
            // else then we don't really care. It will show up in
            // the evaluator as a non-all member and trigger the
            // join when we call RolapNativeSet.isJoinRequired().
            final AtomicBoolean mustJoin = new AtomicBoolean(false);
            filterExpr.accept(
                new MdxVisitorImpl() {
                    public Object visit(MemberExpr memberExpr) {
                        if (memberExpr.getMember().isMeasure()) {
                            mustJoin.set(true);
                            return null;
                        }
                        return super.visit(memberExpr);
                    }
                });
            return mustJoin.get()
                || (getEvaluator().isNonEmpty() && super.isJoinRequired() || existing);
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            // Use aggregate table to generate filter condition
            RolapNativeSql sql =
                new RolapNativeSql(
                    sqlQuery, aggStar, getEvaluator(), args[0].getLevel());
            String filterSql = sql.generateFilterCondition(filterExpr);

            // <NOOP> is used because there is a previous check to make sure filter conditions aren't null,
            // and there is a usecase where the filter is simply "COUNT(Member.CurrentMember, NONEMPTY)"
            // AKA a Non Empty Check, which just needs to join to the fact table.
            if (filterSql != null && !filterSql.equals("<NOOP>") && !filterSql.equals("(<NOOP>) ")) {
                sqlQuery.addHaving(filterSql);
            }
            if (getEvaluator().isNonEmpty() || isJoinRequired()) {
                // only apply context constraint if non empty, or
                // if a join is required to fulfill the filter condition
                super.addConstraint(sqlQuery, baseCube, aggStar);
            }
        }

        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());
            // Note required to use string in order for caching to work
            if (filterExpr != null) {
                key.add(filterExpr.toString());
            }
            key.add(getEvaluator().isNonEmpty());

            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.add(
                    ((RolapEvaluator)this.getEvaluator())
                    .getSlicerMembers());
            }

            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if (!isEnabled()) {
            return null;
        }
        if (!FilterConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }
        // is this "Filter(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Filter".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length != 2) {
            return null;
        }

        // Determine if this Filter wraps an Existing function, of so apply
        // the whole context to the filter.
        boolean existing = false;
        Exp arg0 = args[0];
//        if (args[0] instanceof ResolvedFunCall) {
//            if (((ResolvedFunCall)args[0]).getFunName().equalsIgnoreCase("existing")) {
//                existing = true;
//                arg0 = ((ResolvedFunCall)args[0]).getArg(0);
//            }
//        }

        // extract the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, arg0);

        // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
        // array is the CrossJoin dimensions.  The second array, if any,
        // contains additional constraints on the dimensions. If either the
        // list or the first array is null, then native cross join is not
        // feasible.
        if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the WHERE condition
        // Need to generate where condition here to determine whether
        // or not the filter condition can be created. The filter
        // condition could change to use an aggregate table later in evaluation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeFilter");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, cjArgs[0].getLevel());
        final Exp filterExpr = args[1];
        String filterExprStr = sql.generateFilterCondition(filterExpr);
        if (filterExprStr == null) {
            return null;
        }

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
                evaluator.getNonAllMembers(), true))
        {
            return null;
        }


        final int savepoint = evaluator.savepoint();
        try {
            if (!existing) {
                overrideContext(evaluator, cjArgs, sql.getStoredMeasure());
            } else {
                // exclude the crossjoin args from overriding the context
                overrideContext(evaluator, new CrossJoinArg[]{}, sql.getStoredMeasure());
            }
            // Now construct the TupleConstraint that contains both the CJ
            // dimensions and the additional filter on them.
            CrossJoinArg[] combinedArgs = cjArgs;
            if (allArgs.size() == 2) {
                CrossJoinArg[] predicateArgs = allArgs.get(1);
                if (predicateArgs != null) {
                    // Combined the CJ and the additional predicate args.
                    combinedArgs =
                        Util.appendArrays(cjArgs, predicateArgs);
                }
            }

            SetConstraint constraint =
                new FilterConstraint(combinedArgs, evaluator, filterExpr, existing);
            // constraint may still fail
            if (!isValidFilterConstraint(constraint, evaluator, combinedArgs)) {
                return null;
            }
            LOGGER.debug("using native filter");
            return new SetEvaluator(cjArgs, schemaReader, constraint, sql.getStoredMeasure());
        } finally {
            evaluator.restore(savepoint);
        }

    }

    /**
     * Check if the need to avoid joining with the fact table is compatible
     * with member constraints to get the right results.<br/>
     * A proper solution would involve changing how joins can be handled in
     * {@link SqlConstraintUtils}, until then we better bail out.
     */
    private static boolean isValidFilterConstraint(
      SetConstraint filterConstraint,
      RolapEvaluator evaluator,
      CrossJoinArg[] combinedArgs)
    {
        // in this situation any CrossJoinArg constraints will be skipped
        // to prevent fact table join and non-empty results (MONDRIAN-1133,1694)
        // this may cause wrong results for args with member constraints
        if (!evaluator.isNonEmpty() && !filterConstraint.isJoinRequired()) {
            // known constraining args have members, but this might change
            for (CrossJoinArg cjArg : combinedArgs) {
                if (cjArg.getMembers() != null) {
                    return false;
                }
            }
        }
        return true;
    }

}

// End RolapNativeFilter.java
