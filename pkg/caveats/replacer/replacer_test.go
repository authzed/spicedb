package replacer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/cel-go/cel"
)

func TestReplaceVariable(t *testing.T) {
	tcs := []struct {
		name         string
		expression   string
		oldVarName   string
		newVarName   string
		expectedExpr string
		expectError  bool
	}{
		{
			name:         "simple variable replacement",
			expression:   "x > 10",
			oldVarName:   "x",
			newVarName:   "y",
			expectedExpr: "y > 10",
		},
		{
			name:         "multiple occurrences replacement",
			expression:   "x > 10 && x < 20",
			oldVarName:   "x",
			newVarName:   "value",
			expectedExpr: "value > 10 && value < 20",
		},
		{
			name:         "no replacement needed",
			expression:   "y > 10",
			oldVarName:   "x",
			newVarName:   "y",
			expectedExpr: "y > 10",
		},
		{
			name:         "replacement in nested expression",
			expression:   "has(user.roles) && 'admin' in user.roles && user.age > x",
			oldVarName:   "x",
			newVarName:   "min_age",
			expectedExpr: "has(user.roles) && \"admin\" in user.roles && user.age > min_age",
		},
		{
			name:        "invalid new variable name",
			expression:  "x > 10",
			oldVarName:  "x",
			newVarName:  "123invalid",
			expectError: true,
		},
		{
			name:        "empty expression",
			expression:  "",
			oldVarName:  "x",
			newVarName:  "y",
			expectError: true,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			env, err := cel.NewEnv(
				cel.Variable("x", cel.IntType),
				cel.Variable("y", cel.IntType),
				cel.Variable("value", cel.IntType),
				cel.Variable("min_age", cel.IntType),
				cel.Variable("user", cel.MapType(cel.StringType, cel.DynType)),
			)
			require.NoError(t, err)

			ast, iss := env.Compile(tc.expression)
			if tc.expectError && iss.Err() != nil {
				require.Error(t, iss.Err())
				return
			}
			require.NoError(t, iss.Err())

			result, err := ReplaceVariable(env, ast, tc.oldVarName, tc.newVarName)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)

			// Verify the result by converting back to string
			resultStr, err := cel.AstToString(result)
			require.NoError(t, err)
			require.Equal(t, tc.expectedExpr, resultStr)
		})
	}
}

func TestInlineVariable(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Variable("x", cel.IntType),
		cel.Variable("y", cel.StringType),
	)
	require.NoError(t, err)

	ast, iss := env.Compile("42")
	require.NoError(t, iss.Err())

	t.Run("InlineVariable methods", func(t *testing.T) {
		iv := newInlineVariable("test_var", ast)

		require.Equal(t, "test_var", iv.Name())
		require.Equal(t, "test_var", iv.Alias())
		require.NotNil(t, iv.Expr())
		require.NotNil(t, iv.Type())
	})

	t.Run("InlineVariable with custom alias", func(t *testing.T) {
		iv := newInlineVariableWithAlias("test_var", "custom_alias", ast)

		require.Equal(t, "test_var", iv.Name())
		require.Equal(t, "custom_alias", iv.Alias())
		require.NotNil(t, iv.Expr())
		require.NotNil(t, iv.Type())
	})
}

func TestNewInlineVariableFunctions(t *testing.T) {
	env, err := cel.NewEnv(cel.Variable("x", cel.IntType))
	require.NoError(t, err)

	ast, iss := env.Compile("100")
	require.NoError(t, iss.Err())

	t.Run("newInlineVariable", func(t *testing.T) {
		iv := newInlineVariable("myvar", ast)
		require.NotNil(t, iv)
		require.Equal(t, "myvar", iv.name)
		require.Equal(t, "myvar", iv.alias)
		require.NotNil(t, iv.def)
	})

	t.Run("newInlineVariableWithAlias", func(t *testing.T) {
		iv := newInlineVariableWithAlias("myvar", "myalias", ast)
		require.NotNil(t, iv)
		require.Equal(t, "myvar", iv.name)
		require.Equal(t, "myalias", iv.alias)
		require.NotNil(t, iv.def)
	})
}

func TestInliningOptimizer(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Variable("x", cel.IntType),
		cel.Variable("y", cel.IntType),
	)
	require.NoError(t, err)

	t.Run("optimizer with single variable", func(t *testing.T) {
		originalAst, iss := env.Compile("x + 10")
		require.NoError(t, iss.Err())

		replacementAst, iss := env.Compile("y")
		require.NoError(t, iss.Err())

		inlineVar := newInlineVariable("x", replacementAst)
		opt := newModifiedInliningOptimizer(inlineVar)

		// Use cel.NewStaticOptimizer to properly create the optimizer context
		staticOpt := cel.NewStaticOptimizer(opt)
		result, iss := staticOpt.Optimize(env, originalAst)

		require.NoError(t, iss.Err())
		require.NotNil(t, result)
	})

	t.Run("optimizer with multiple variables", func(t *testing.T) {
		originalAst, iss := env.Compile("x + y")
		require.NoError(t, iss.Err())

		xReplacementAst, iss := env.Compile("5")
		require.NoError(t, iss.Err())

		yReplacementAst, iss := env.Compile("15")
		require.NoError(t, iss.Err())

		inlineVarX := newInlineVariable("x", xReplacementAst)
		inlineVarY := newInlineVariable("y", yReplacementAst)
		opt := newModifiedInliningOptimizer(inlineVarX, inlineVarY)

		staticOpt := cel.NewStaticOptimizer(opt)
		result, iss := staticOpt.Optimize(env, originalAst)

		require.NoError(t, iss.Err())
		require.NotNil(t, result)
	})

	t.Run("optimizer with no matching variables", func(t *testing.T) {
		originalAst, iss := env.Compile("x + 10")
		require.NoError(t, iss.Err())

		replacementAst, iss := env.Compile("100")
		require.NoError(t, iss.Err())

		// Use a variable that doesn't exist in the expression
		inlineVar := newInlineVariable("nonexistent", replacementAst)
		opt := newModifiedInliningOptimizer(inlineVar)

		staticOpt := cel.NewStaticOptimizer(opt)
		result, iss := staticOpt.Optimize(env, originalAst)

		require.NoError(t, iss.Err())
		require.NotNil(t, result)

		// The result should be functionally the same as the original since no variables matched
		originalStr, err := cel.AstToString(originalAst)
		require.NoError(t, err)
		resultStr, err := cel.AstToString(result)
		require.NoError(t, err)
		require.Equal(t, originalStr, resultStr)
	})
}
