package ast

// Expressions
type Expression interface {
	GetValue() interface{}
}

type WhereExpression struct {
	Left  Expression
	Right Expression
	Op    string
}

func (w *WhereExpression) GetValue() interface{} {
	return w.Right.GetValue()
}

type AllExpression struct {
}

func (a *AllExpression) GetValue() interface{} {
	return nil
}

type Identifier struct {
	Value string
}

func (i *Identifier) GetValue() interface{} {
	return i.Value
}

type StringLiteral struct {
	Value string
}

func (s *StringLiteral) GetValue() interface{} {
	return s.Value
}

type Int64Literal struct {
	Value int64
}

func (i *Int64Literal) GetValue() interface{} {
	return i.Value
}

type Float64Literal struct {
	Value float64
}

func (f *Float64Literal) GetValue() interface{} {
	return f.Value
}

type BooleanLiteral struct {
	Value bool
}

func (b *BooleanLiteral) GetValue() interface{} {
	return b.Value
}

type Literal struct {
	Value interface{}
}

func (l *Literal) GetValue() interface{} {
	return l.Value
}

type VariableExpression struct {
	Name string
}

func (v *VariableExpression) GetValue() interface{} {
	return v.Name
}

type BinaryExpression struct {
	Left  Expression
	Right Expression
	Op    string
}

func (b *BinaryExpression) GetValue() interface{} {
	return nil
}
