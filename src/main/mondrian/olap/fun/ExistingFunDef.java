/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.IterCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;
import mondrian.rolap.ManyToManyUtil;
import mondrian.rolap.RolapEvaluator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExistingFunDef extends FunDefBase {

    static final ExistingFunDef instance = new ExistingFunDef();

    protected ExistingFunDef() {
      super(
          "Existing",
          "Existing <Set>",
          "Forces the set to be evaluated within the current context.",
          "Pxx");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return args[0].getType();
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final IterCalc setArg = compiler.compileIter(call.getArg(0));
         final Type myType = call.getArg(0).getType();

        return new AbstractListCalc(call, new Calc[] {setArg}) {
            public boolean dependsOn(Hierarchy hierarchy) {
                // TODO: REVIEW definitely arg in SetType#usesHierarchy
                // Note, this is used by native evaluation.
                // otherwise the native evaluator will override the current
                // context to the default.
//                return true;
                boolean argsDepend = super.dependsOn(hierarchy);
                return argsDepend || myType.usesHierarchy(hierarchy, false);
            }

            public TupleList evaluateList(Evaluator evaluator) {
                RolapEvaluator manyToManyEval =
                    ManyToManyUtil.getManyToManyEvaluator(
                        (RolapEvaluator)evaluator);
                NativeEvaluator nativeEvaluator =
                    evaluator.getSchemaReader().getNativeSetEvaluator(
                        call.getFunDef(),
                        call.getArgs(),
                        manyToManyEval,
                        this);
                if (nativeEvaluator != null) {
                    return (TupleList) nativeEvaluator.execute(ResultStyle.LIST);
                } else {
                  TupleIterable setTuples = setArg.evaluateIterable(evaluator);
  
                  TupleList result =
                      TupleCollections.createList(setTuples.getArity());
                  List<Member> contextMembers =
                      Arrays.asList(evaluator.getMembers());
  
                  List<Hierarchy> argDims = null;
                  List<Hierarchy> contextDims = getHierarchies(contextMembers);
  
                  for (List<Member> tuple : setTuples) {
                      if (argDims == null) {
                          argDims = getHierarchies(tuple);
                      }
                      if (existsInTuple(tuple, contextMembers,
                          argDims, contextDims, evaluator))
                      {
                          result.add(tuple);
                      }
                  }
                  return result;
              }
            }
        };
    }

    private static List<Hierarchy> getHierarchies(final List<Member> members)
    {
        List<Hierarchy> hierarchies = new ArrayList<Hierarchy>(members.size());
        for (Member member : members) {
            hierarchies.add(member.getHierarchy());
        }
        return hierarchies;
    }

}
// End ExistingFunDef.java