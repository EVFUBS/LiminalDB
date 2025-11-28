package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	ops "LiminalDb/internal/database/operations"
	"LiminalDb/internal/interpreter"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// sqlRequest mirrors the request body expected by the HTTP /exec endpoint.
type sqlRequest struct {
	SQL string `json:"sql"`
}

// sqlResponse mirrors the server's /exec response, which wraps an
// operations.Result. We then pass that through the shared formatting logic
// from the interpreter REPL so the TUI output matches the REPL.
type sqlResponse struct {
	Success bool       `json:"success"`
	Result  ops.Result `json:"result"`
}

// execMsg is a Bubble Tea message carrying the result of an executed SQL
// statement.
type execMsg struct {
	output string
	err    error
}

// execSQLCmd wraps executeSQL in a Bubble Tea command so it can run
// asynchronously and send the result back to the Update loop.
func execSQLCmd(addr, sql string) tea.Cmd {
	return func() tea.Msg {
		out, err := executeSQL(addr, sql)
		return execMsg{output: out, err: err}
	}
}

// key mappings for the TUI.
type keyMap struct {
	Quit key.Binding
	Run  key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		Quit: key.NewBinding(
			key.WithKeys("ctrl+c", "esc", ":q"),
			key.WithHelp("ctrl+c", "quit"),
		),
		Run: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "execute SQL"),
		),
	}
}

// ShortHelp returns keybindings to show in the minimized help view.
func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Run, k.Quit}
}

// FullHelp returns keybindings for the expanded help view.
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Run},
		{k.Quit},
	}
}

// Styles for the UI.
var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	subtle      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	errorStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	statusStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("44")).Bold(true)
	boxStyle    = lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("240")).Padding(0, 1)
)

type model struct {
	addr     string
	input    textarea.Model
	viewport viewport.Model
	help     help.Model
	keys     keyMap
	status   string
	loading  bool
	err      error
	width    int
	height   int
}

func newModel(addr string) model {
	ta := textarea.New()
	ta.Placeholder = "Type SQL here. Use :q or :quit to exit."
	ta.Focus()
	ta.Prompt = "SQL> "
	ta.CharLimit = 0
	ta.FocusedStyle.CursorLine = ta.FocusedStyle.CursorLine.Background(lipgloss.Color("236"))
	ta.ShowLineNumbers = false

	vp := viewport.New(80, 20)
	vp.SetContent(subtle.Render("Results will appear here."))

	h := help.New()
	h.ShowAll = true

	return model{
		addr:     addr,
		input:    ta,
		viewport: vp,
		help:     h,
		keys:     newKeyMap(),
		status:   "Connected to " + addr,
	}
}

// Init satisfies the tea.Model interface.
func (m model) Init() tea.Cmd {
	return textarea.Blink
}

// Update satisfies the tea.Model interface.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height

		// We build the layout as:
		//   title
		//   server address
		//   blank line
		//   "SQL input:" label
		//   input box
		//   blank line
		//   "Results:" label
		//   results viewport
		//   blank line
		//   status line
		//   help (usually 1–2 lines)
		//
		// To ensure the top of the SQL input box is always visible, we
		// explicitly budget a fixed number of lines for everything *except*
		// the input box and results viewport, then divide the remaining space
		// between them. This keeps the total height <= terminal height.
		const chromeLines = 10 // headers, labels, blanks, status, help
		const minInputHeight = 3
		const minResultsHeight = 3

		available := m.height - chromeLines
		if available < 1 {
			available = 1
		}

		var inputHeight, resultsHeight int
		if available <= minInputHeight+minResultsHeight {
			// Very small terminals: split space roughly in half.
			inputHeight = available / 2
			if inputHeight < 1 {
				inputHeight = 1
			}
			resultsHeight = available - inputHeight
			if resultsHeight < 1 {
				resultsHeight = 1
			}
		} else {
			// Normal case: give 1/3 to input, 2/3 to results.
			inputHeight = available / 3
			if inputHeight < minInputHeight {
				inputHeight = minInputHeight
			}
			resultsHeight = available - inputHeight
			if resultsHeight < minResultsHeight {
				resultsHeight = minResultsHeight
			}
		}

		m.input.SetWidth(m.width - 6)
		m.input.SetHeight(inputHeight)
		m.viewport.Width = m.width - 6
		m.viewport.Height = resultsHeight
	case tea.KeyMsg:
		keyStr := msg.String()
		// Support REPL-style :q / :quit commands as first token.
		if keyStr == ":" {
			// Let textarea handle it normally.
			break
		}

		if key.Matches(msg, m.keys.Quit) {
			return m, tea.Quit
		}

		if key.Matches(msg, m.keys.Run) {
			// On Enter, try to execute the current input buffer as SQL.
			line := strings.TrimSpace(m.input.Value())
			if line == "" {
				break
			}
			if line == ":q" || line == ":quit" {
				return m, tea.Quit
			}

			m.loading = true
			m.status = "Executing..."
			m.err = nil

			cmds = append(cmds, execSQLCmd(m.addr, line))
			break
		}
	case execMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			m.status = "Execution failed"
			m.viewport.SetContent(errorStyle.Render(msg.err.Error()))
		} else {
			m.err = nil
			m.status = "Execution succeeded"
			m.viewport.SetContent(msg.output)
		}
	}

	// Let components update themselves.
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	cmds = append(cmds, cmd)
	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

// View draws the entire interface.
func (m model) View() string {
	title := titleStyle.Render("LiminalDB") + " " + subtle.Render("TUI client")
	addr := subtle.Render("Server: " + m.addr)

	inputBox := boxStyle.Render(m.input.View())
	resultBox := boxStyle.Render(m.viewport.View())

	status := m.status
	if m.loading {
		status += " (working...)"
	}
	statusLine := statusStyle.Render(status)
	if m.err != nil {
		statusLine += "  " + errorStyle.Render(m.err.Error())
	}

	helpView := m.help.View(m.keys)

	return lipgloss.JoinVertical(lipgloss.Left,
		title,
		addr,
		"",
		"SQL input:",
		inputBox,
		"",
		"Results:",
		resultBox,
		"",
		statusLine,
		helpView,
	)
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "LiminalDB server address")
	flag.Parse()

	if !strings.HasPrefix(*addr, "http://") && !strings.HasPrefix(*addr, "https://") {
		*addr = "http://" + *addr
	}

	m := newModel(*addr)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Println("Error running TUI:", err)
		os.Exit(1)
	}
}

// executeSQL performs the HTTP call to the server and returns a pretty-printed
// JSON string (or plain text on failure) along with any error.
func executeSQL(addr, sql string) (string, error) {
	reqBody, err := json.Marshal(sqlRequest{SQL: sql})
	if err != nil {
		return "", fmt.Errorf("failed to encode request: %w", err)
	}

	url := strings.TrimRight(addr, "/") + "/exec"
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// The server returns error text in the body; surface it.
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return "", fmt.Errorf("server error (%d): %s", resp.StatusCode, msg)
	}

	var sr sqlResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	// Use the same human-readable formatting as the REPL so that tables and
	// query results look identical between the CLI and the TUI.
	formatted := interpreter.FormatResult(sr.Result)
	return formatted, nil
}
