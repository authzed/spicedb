package pg_query

func MakeStrNode(str string) *Node {
	return &Node{Node: &Node_String_{String_: &String{Sval: str}}}
}

func MakeAConstStrNode(str string, location int32) *Node {
	return &Node{
		Node: &Node_AConst{
			AConst: &A_Const{
				Val: &A_Const_Sval{
					Sval: &String{Sval: str},
				},
				Isnull:   false,
				Location: location,
			},
		},
	}
}

func MakeIntNode(ival int64) *Node {
	return &Node{Node: &Node_Integer{Integer: &Integer{Ival: int32(ival)}}}
}

func MakeAConstIntNode(ival int64, location int32) *Node {
	return &Node{
		Node: &Node_AConst{
			AConst: &A_Const{
				Val: &A_Const_Ival{
					Ival: &Integer{Ival: int32(ival)},
				},
				Isnull:   false,
				Location: location,
			},
		},
	}
}

func MakeListNode(items []*Node) *Node {
	return &Node{Node: &Node_List{List: &List{Items: items}}}
}

func MakeResTargetNodeWithName(name string, location int32) *Node {
	return &Node{Node: &Node_ResTarget{ResTarget: &ResTarget{Name: name, Location: location}}}
}

func MakeResTargetNodeWithVal(val *Node, location int32) *Node {
	return &Node{Node: &Node_ResTarget{ResTarget: &ResTarget{Val: val, Location: location}}}
}

func MakeResTargetNodeWithNameAndVal(name string, val *Node, location int32) *Node {
	return &Node{Node: &Node_ResTarget{ResTarget: &ResTarget{Name: name, Val: val, Location: location}}}
}

func MakeSimpleRangeVar(relname string, location int32) *RangeVar {
	return &RangeVar{
		Relname:        relname,
		Inh:            true,
		Relpersistence: "p",
		Location:       location,
	}
}

func MakeSimpleRangeVarNode(relname string, location int32) *Node {
	return &Node{
		Node: &Node_RangeVar{
			RangeVar: MakeSimpleRangeVar(relname, location),
		},
	}
}

func MakeFullRangeVar(schemaname string, relname string, alias string, location int32) *RangeVar {
	return &RangeVar{
		Schemaname:     schemaname,
		Relname:        relname,
		Inh:            true,
		Relpersistence: "p",
		Alias: &Alias{
			Aliasname: alias,
		},
		Location: location,
	}
}

func MakeFullRangeVarNode(schemaname string, relname string, alias string, location int32) *Node {
	return &Node{
		Node: &Node_RangeVar{
			RangeVar: MakeFullRangeVar(schemaname, relname, alias, location),
		},
	}
}

func MakeParamRefNode(number int32, location int32) *Node {
	return &Node{
		Node: &Node_ParamRef{
			ParamRef: &ParamRef{Number: number, Location: location},
		},
	}
}

func MakeColumnRefNode(fields []*Node, location int32) *Node {
	return &Node{
		Node: &Node_ColumnRef{
			ColumnRef: &ColumnRef{Fields: fields, Location: location},
		},
	}
}

func MakeAStarNode() *Node {
	return &Node{
		Node: &Node_AStar{
			AStar: &A_Star{},
		},
	}
}

func MakeCaseExprNode(arg *Node, args []*Node, location int32) *Node {
	return &Node{
		Node: &Node_CaseExpr{
			CaseExpr: &CaseExpr{
				Arg:      arg,
				Args:     args,
				Location: location,
			},
		},
	}
}

func MakeCaseWhenNode(expr *Node, result *Node, location int32) *Node {
	return &Node{
		Node: &Node_CaseWhen{
			CaseWhen: &CaseWhen{
				Expr:     expr,
				Result:   result,
				Location: location,
			},
		},
	}
}

func MakeFuncCallNode(funcname []*Node, args []*Node, location int32) *Node {
	return &Node{
		Node: &Node_FuncCall{
			FuncCall: &FuncCall{
				Funcname:   funcname,
				Args:       args,
				Funcformat: CoercionForm_COERCE_EXPLICIT_CALL,
				Location:   location,
			},
		},
	}
}

func MakeJoinExprNode(jointype JoinType, larg *Node, rarg *Node, quals *Node) *Node {
	return &Node{
		Node: &Node_JoinExpr{
			JoinExpr: &JoinExpr{
				Jointype: jointype,
				Larg:     larg,
				Rarg:     rarg,
				Quals:    quals,
			},
		},
	}
}

func MakeAExprNode(kind A_Expr_Kind, name []*Node, lexpr *Node, rexpr *Node, location int32) *Node {
	return &Node{
		Node: &Node_AExpr{
			AExpr: &A_Expr{
				Kind:     kind,
				Name:     name,
				Lexpr:    lexpr,
				Rexpr:    rexpr,
				Location: location,
			},
		},
	}
}

func MakeBoolExprNode(boolop BoolExprType, args []*Node, location int32) *Node {
	return &Node{
		Node: &Node_BoolExpr{
			BoolExpr: &BoolExpr{
				Boolop:   boolop,
				Args:     args,
				Location: location,
			},
		},
	}
}

func MakeSortByNode(node *Node, sortbyDir SortByDir, sortbyNulls SortByNulls, location int32) *Node {
	return &Node{
		Node: &Node_SortBy{
			SortBy: &SortBy{
				Node:        node,
				SortbyDir:   sortbyDir,
				SortbyNulls: sortbyNulls,
				Location:    location,
			},
		},
	}
}

func MakeSimpleDefElemNode(defname string, arg *Node, location int32) *Node {
	return &Node{
		Node: &Node_DefElem{
			DefElem: &DefElem{
				Defname:   defname,
				Arg:       arg,
				Defaction: DefElemAction_DEFELEM_UNSPEC,
				Location:  location,
			},
		},
	}
}

func MakeSimpleColumnDefNode(colname string, typeName *TypeName, constraints []*Node, location int32) *Node {
	return &Node{
		Node: &Node_ColumnDef{
			ColumnDef: &ColumnDef{
				Colname:     colname,
				TypeName:    typeName,
				Constraints: constraints,
				IsLocal:     true,
				Location:    location,
			},
		},
	}
}

func MakePrimaryKeyConstraintNode(location int32) *Node {
	return &Node{
		Node: &Node_Constraint{
			Constraint: &Constraint{
				Contype:  ConstrType_CONSTR_PRIMARY,
				Location: location,
			},
		},
	}
}

func MakeNotNullConstraintNode(location int32) *Node {
	return &Node{
		Node: &Node_Constraint{
			Constraint: &Constraint{
				Contype:  ConstrType_CONSTR_NOTNULL,
				Location: location,
			},
		},
	}
}

func MakeDefaultConstraintNode(rawExpr *Node, location int32) *Node {
	return &Node{
		Node: &Node_Constraint{
			Constraint: &Constraint{
				Contype:  ConstrType_CONSTR_DEFAULT,
				RawExpr:  rawExpr,
				Location: location,
			},
		},
	}
}

func MakeSimpleRangeFunctionNode(functions []*Node) *Node {
	return &Node{
		Node: &Node_RangeFunction{
			RangeFunction: &RangeFunction{
				Functions: functions,
			},
		},
	}
}
