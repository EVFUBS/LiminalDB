package sqlparser

import (
	"LiminalDb/internal/db"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Repl() {
	fmt.Println("Welcome to LiminalDB")
	fmt.Println("Enter SQL commands, or type 'exit' to quit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		if strings.ToLower(input) == "exit" {
			break
		}

		if input == "" {
			continue
		}

		lexer := NewLexer(input)
		parser := NewParser(lexer)
		evaluator := NewEvaluator(parser)

		result, err := evaluator.Execute(input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		formattedResult := formatResult(result)
		fmt.Println(formattedResult)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}

func formatResult(result interface{}) string {
	switch v := result.(type) {
	case *db.Table:
		return formatTableResult(v)
	case db.TableMetadata:
		return formatTableMetadata(v)
	case *db.QueryResult:
		return formatQueryResult(v)
	case string:
		return v // Already formatted messages like "Insert successful"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatTableResult(table *db.Table) string {
	if len(table.Data) == 0 {
		return "Empty set"
	}

	var sb strings.Builder

	// Calculate column widths
	colWidths := make([]int, len(table.Metadata.Columns))
	for i, col := range table.Metadata.Columns {
		colWidths[i] = len(col.Name)
		// Check data lengths
		for _, row := range table.Data {
			if i < len(row) {
				valLen := len(fmt.Sprintf("%v", row[i]))
				if valLen > colWidths[i] {
					colWidths[i] = valLen
				}
			}
		}
	}

	// Print header
	sb.WriteString("+")
	for _, width := range colWidths {
		sb.WriteString(strings.Repeat("-", width+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n|")

	// Column names
	for i, col := range table.Metadata.Columns {
		sb.WriteString(fmt.Sprintf(" %-*s |", colWidths[i], col.Name))
	}
	sb.WriteString("\n+")

	// Separator
	for _, width := range colWidths {
		sb.WriteString(strings.Repeat("-", width+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n")

	// Data rows
	for _, row := range table.Data {
		sb.WriteString("|")
		for i, val := range row {
			if i < len(colWidths) {
				sb.WriteString(fmt.Sprintf(" %-*v |", colWidths[i], formatValue(val)))
			}
		}
		sb.WriteString("\n")
	}

	// Bottom border
	sb.WriteString("+")
	for _, width := range colWidths {
		sb.WriteString(strings.Repeat("-", width+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n")

	// Row count
	sb.WriteString(fmt.Sprintf("%d row(s) in set\n", len(table.Data)))

	return sb.String()
}

func formatTableMetadata(metadata db.TableMetadata) string {
	var sb strings.Builder

	// Calculate column widths
	nameWidth := len("Field")
	typeWidth := len("Type")
	nullWidth := len("Null")

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
	sb.WriteString(fmt.Sprintf("+%-*s+%-*s+%-*s+\n",
		nameWidth+2, strings.Repeat("-", nameWidth+2),
		typeWidth+2, strings.Repeat("-", typeWidth+2),
		nullWidth+2, strings.Repeat("-", nullWidth+2)))

	// Column headers
	sb.WriteString(fmt.Sprintf("| %-*s | %-*s | %-*s |\n",
		nameWidth, "Field",
		typeWidth, "Type",
		nullWidth, "Null"))

	// Separator
	sb.WriteString(fmt.Sprintf("+%-*s+%-*s+%-*s+\n",
		nameWidth+2, strings.Repeat("-", nameWidth+2),
		typeWidth+2, strings.Repeat("-", typeWidth+2),
		nullWidth+2, strings.Repeat("-", nullWidth+2)))

	// Data rows
	for _, col := range metadata.Columns {
		nullable := "NO"
		if col.IsNullable {
			nullable = "YES"
		}

		sb.WriteString(fmt.Sprintf("| %-*s | %-*s | %-*s |\n",
			nameWidth, col.Name,
			typeWidth, formatColumnType(col),
			nullWidth, nullable))
	}

	// Bottom border
	sb.WriteString(fmt.Sprintf("+%-*s+%-*s+%-*s+\n",
		nameWidth+2, strings.Repeat("-", nameWidth+2),
		typeWidth+2, strings.Repeat("-", typeWidth+2),
		nullWidth+2, strings.Repeat("-", nullWidth+2)))

	return sb.String()
}

func formatQueryResult(result *db.QueryResult) string {
	if len(result.Rows) == 0 {
		return "No rows found"
	}

	var sb strings.Builder

	sb.WriteString("+-----------------+\n")
	for i := range result.Columns {
		sb.WriteString(fmt.Sprintf("| %-15s ", result.Columns[i].Name))
	}
	sb.WriteString("|\n")
	sb.WriteString("+-----------------+\n")

	for _, row := range result.Rows {
		for _, value := range row {
			sb.WriteString(fmt.Sprintf("| %-15v ", value))
		}
		sb.WriteString("|\n")
	}
	sb.WriteString("+-----------------+\n")

	return sb.String()
}

func formatColumnType(col db.Column) string {
	switch col.DataType {
	case db.TypeString:
		if col.Length > 0 {
			return fmt.Sprintf("STRING(%d)", col.Length)
		}
		return "STRING"
	case db.TypeInteger64:
		return "INT"
	case db.TypeFloat64:
		return "FLOAT"
	case db.TypeBoolean:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}
