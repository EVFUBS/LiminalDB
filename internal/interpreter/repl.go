package interpreter

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/logger"
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Helper functions for table formatting
func calculateColumnWidths(columns []database.Column, rows [][]any) []int {
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col.Name)
		for _, row := range rows {
			if i < len(row) {
				valLen := len(fmt.Sprintf("%v", row[i]))
				if valLen > colWidths[i] {
					colWidths[i] = valLen
				}
			}
		}
	}
	return colWidths
}

func writeTableHeader(sb *strings.Builder, colWidths []int) {
	sb.WriteString("+")
	for _, width := range colWidths {
		sb.WriteString(strings.Repeat("-", width+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n")
}

func writeTableFooter(sb *strings.Builder, colWidths []int) {
	sb.WriteString("+")
	for _, width := range colWidths {
		sb.WriteString(strings.Repeat("-", width+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n")
}

func writeColumnNames(sb *strings.Builder, columns []database.Column, colWidths []int) {
	sb.WriteString("|")
	for i, col := range columns {
		fmt.Fprintf(sb, " %-*s |", colWidths[i], col.Name)
	}
	sb.WriteString("\n")
}

func writeDataRow(sb *strings.Builder, row []any, colWidths []int) {
	sb.WriteString("|")
	for i, val := range row {
		if i < len(colWidths) {
			fmt.Fprintf(sb, " %-*v |", colWidths[i], formatValue(val))
		}
	}
	sb.WriteString("\n")
}

func Repl() {
	logger.Info("Starting REPL session")
	fmt.Println("Welcome to LiminalDB")
	fmt.Println("Enter SQL commands, or type 'exit' to quit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			logger.Error("Error reading input: %v", scanner.Err())
			break
		}

		input := scanner.Text()

		if strings.ToLower(input) == "exit" {
			logger.Info("User requested exit")
			break
		}

		if input == "" {
			continue
		}

		logger.Debug("Processing command: %s", input)

		result, err := Execute(input)
		if err != nil {
			logger.Error("Command execution failed: %v", err)
			fmt.Printf("Error: %v\n", err)
			continue
		}

		logger.Debug("Command executed successfully")
		formattedResult := formatResult(result)
		fmt.Println(formattedResult)
	}

	if err := scanner.Err(); err != nil {
		logger.Error("Error reading input: %v", err)
		fmt.Printf("Error reading input: %v\n", err)
	}

	logger.Info("REPL session ended")
}

func Execute(sql string) (any, error) {
	lexer := NewLexer(sql)
	parser := NewParser(lexer)
	evaluator := NewEvaluator(parser)
	return evaluator.Execute(sql)
}

// Result formatting functions
func formatResult(result any) string {
	switch v := result.(type) {
	case *database.Table:
		return formatTableResult(v)
	case database.TableMetadata:
		return formatTableMetadata(v)
	case *database.QueryResult:
		return formatQueryResult(v)
	case string:
		return v // Already formatted messages like "Insert successful"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatTableResult(table *database.Table) string {
	if len(table.Data) == 0 {
		return "Empty set"
	}

	var sb strings.Builder
	colWidths := calculateColumnWidths(table.Metadata.Columns, table.Data)

	writeTableHeader(&sb, colWidths)
	writeColumnNames(&sb, table.Metadata.Columns, colWidths)
	writeTableFooter(&sb, colWidths)

	for _, row := range table.Data {
		writeDataRow(&sb, row, colWidths)
	}

	writeTableFooter(&sb, colWidths)
	sb.WriteString(fmt.Sprintf("%d row(s) in set\n", len(table.Data)))

	return sb.String()
}

func formatTableMetadata(metadata database.TableMetadata) string {
	var sb strings.Builder

	// Calculate column widths
	nameWidth := len("Field")
	typeWidth := len("Type")
	nullWidth := len("Null")
	primaryWidth := len("Primary Key")

	for _, col := range metadata.Columns {
		if len(col.Name) > nameWidth {
			nameWidth = len(col.Name)
		}
		typeStr := formatColumnType(col)
		if len(typeStr) > typeWidth {
			typeWidth = len(typeStr)
		}
	}

	// Header
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", nameWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", typeWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", nullWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", primaryWidth+2))
	sb.WriteString("+\n")

	// Column headers
	sb.WriteString("| ")
	sb.WriteString(fmt.Sprintf("%-*s", nameWidth, "Field"))
	sb.WriteString(" | ")
	sb.WriteString(fmt.Sprintf("%-*s", typeWidth, "Type"))
	sb.WriteString(" | ")
	sb.WriteString(fmt.Sprintf("%-*s", nullWidth, "Null"))
	sb.WriteString(" | ")
	sb.WriteString(fmt.Sprintf("%-*s", primaryWidth, "Primary Key"))
	sb.WriteString(" |\n")

	// Separator
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", nameWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", typeWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", nullWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", primaryWidth+2))
	sb.WriteString("+\n")

	// Data rows
	for _, col := range metadata.Columns {
		nullable := "YES"
		if !col.IsNullable {
			nullable = "NO"
		}
		primary := "NO"
		if col.IsPrimaryKey {
			primary = "YES"
		}

		sb.WriteString("| ")
		sb.WriteString(fmt.Sprintf("%-*s", nameWidth, col.Name))
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%-*s", typeWidth, formatColumnType(col)))
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%-*s", nullWidth, nullable))
		sb.WriteString(" | ")
		sb.WriteString(fmt.Sprintf("%-*s", primaryWidth, primary))
		sb.WriteString(" |\n")
	}

	// Bottom border
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", nameWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", typeWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", nullWidth+2))
	sb.WriteString("+")
	sb.WriteString(strings.Repeat("-", primaryWidth+2))
	sb.WriteString("+\n")

	return sb.String()
}

func formatQueryResult(result *database.QueryResult) string {
	if len(result.Rows) == 0 {
		return "No rows found"
	}

	var sb strings.Builder
	colWidths := calculateColumnWidths(result.Columns, result.Rows)

	writeTableHeader(&sb, colWidths)
	writeColumnNames(&sb, result.Columns, colWidths)
	writeTableFooter(&sb, colWidths)

	for _, row := range result.Rows {
		writeDataRow(&sb, row, colWidths)
	}

	writeTableFooter(&sb, colWidths)
	sb.WriteString(fmt.Sprintf("%d row(s) in set\n", len(result.Rows)))

	return sb.String()
}

// Utility functions
func formatColumnType(col database.Column) string {
	switch col.DataType {
	case database.TypeString:
		if col.Length > 0 {
			return fmt.Sprintf("STRING(%d)", col.Length)
		}
		return "STRING"
	case database.TypeInteger64:
		return "INT"
	case database.TypeFloat64:
		return "FLOAT"
	case database.TypeBoolean:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}
