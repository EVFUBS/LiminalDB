package ast

import "time"

type Expression interface {
	GetValue() any
}

type AssignmentExpression struct {
	Left  Expression
	Right Expression
	Op    string
}

func (w *AssignmentExpression) GetValue() any {
	return w.Right.GetValue()
}

type AllExpression struct{}

func (a *AllExpression) GetValue() any {
	return nil
}

type Identifier struct {
	Value string
}

func (i *Identifier) GetValue() any {
	return i.Value
}

type Literal struct {
	Value any
}

func (l *Literal) GetValue() any {
	return l.Value
}

type StringLiteral struct {
	Value string
}

func (s *StringLiteral) GetValue() any {
	return s.Value
}

type Int64Literal struct {
	Value int64
}

func (i *Int64Literal) GetValue() any {
	return i.Value
}

type Float64Literal struct {
	Value float64
}

func (f *Float64Literal) GetValue() any {
	return f.Value
}

type BooleanLiteral struct {
	Value bool
}

func (b *BooleanLiteral) GetValue() any {
	return b.Value
}

type DateTimeLiteral struct {
	Value time.Time
}

func (d *DateTimeLiteral) GetValue() any {
	return d.Value
}

type VariableExpression struct {
	Name string
}

func (v *VariableExpression) GetValue() any {
	return v.Name
}

type BinaryExpression struct {
	Left  Expression
	Right Expression
	Op    string
}

func (b *BinaryExpression) GetValue() any {
	return nil
}
