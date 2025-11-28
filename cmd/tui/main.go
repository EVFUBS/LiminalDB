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
	sql    string
}

// execSQLCmd wraps executeSQL in a Bubble Tea command so it can run
// asynchronously and send the result back to the Update loop.
func execSQLCmd(addr, sql string) tea.Cmd {
	return func() tea.Msg {
		out, err := executeSQL(addr, sql)
		return execMsg{output: out, err: err, sql: sql}
	}
}

// key mappings for the TUI.
type keyMap struct {
	Quit        key.Binding
	Run         key.Binding
	HistoryUp   key.Binding
	HistoryDown key.Binding
	Clear       key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		Quit: key.NewBinding(
			key.WithKeys("ctrl+c", "esc"),
			key.WithHelp("ctrl+c", "quit"),
		),
		Run: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "run"),
		),
		HistoryUp: key.NewBinding(
			key.WithKeys("alt+up"),
			key.WithHelp("alt+up", "history prev"),
		),
		HistoryDown: key.NewBinding(
			key.WithKeys("alt+down"),
			key.WithHelp("alt+down", "history next"),
		),
		Clear: key.NewBinding(
			key.WithKeys("ctrl+l"),
			key.WithHelp("ctrl+l", "clear output"),
		),
	}
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Run, k.Quit, k.Clear, k.HistoryUp}
}

func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Run, k.Quit, k.Clear},
		{k.HistoryUp, k.HistoryDown},
	}
}

// Styles for the UI.
var (
	appStyle = lipgloss.NewStyle().Padding(0, 0)

	// Brand colors
	primaryColor   = lipgloss.Color("#7D56F4") // Vibrant Purple
	secondaryColor = lipgloss.Color("#5B3C88") // Darker Purple
	successColor   = lipgloss.Color("#04B575") // Green
	errorColor     = lipgloss.Color("#FF3E3E") // Red
	subtleColor    = lipgloss.Color("#626262") // Gray
	textColor      = lipgloss.Color("#FAFAFA") // White-ish

	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(primaryColor).
			Padding(0, 1).
			Bold(true)

	// Status Bar Styles
	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Padding(0, 1)

	statusOKStyle      = statusBarStyle.Copy().Background(successColor)
	statusErrorStyle   = statusBarStyle.Copy().Background(errorColor)
	statusLoadingStyle = statusBarStyle.Copy().Background(secondaryColor)

	// Viewport Styles
	viewportStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(subtleColor).
			Padding(0, 1)

	// Input Styles
	inputStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(0, 1)

	// Text Styles
	errorMessageStyle = lipgloss.NewStyle().Foreground(errorColor)
	commandStyle      = lipgloss.NewStyle().Foreground(subtleColor)
)

type model struct {
	addr       string
	input      textarea.Model
	viewport   viewport.Model
	help       help.Model
	keys       keyMap
	status     string
	loading    bool
	err        error
	width      int
	height     int
	history    []string
	historyIdx int
}

func newModel(addr string) model {
	ta := textarea.New()
	ta.Placeholder = "SELECT * FROM users..."
	ta.Focus()
	ta.Prompt = "SQL> "
	ta.CharLimit = 0
	ta.ShowLineNumbers = false
	ta.SetHeight(3)

	// Textarea styling
	ta.FocusedStyle.CursorLine = lipgloss.NewStyle() // No background, just clean text
	ta.FocusedStyle.Prompt = lipgloss.NewStyle().Foreground(primaryColor)
	ta.FocusedStyle.Base = lipgloss.NewStyle().Foreground(textColor)

	vp := viewport.New(80, 20)
	vp.SetContent("Welcome to LiminalDB TUI.\nType your SQL queries below.")

	h := help.New()

	return model{
		addr:       addr,
		input:      ta,
		viewport:   vp,
		help:       h,
		keys:       newKeyMap(),
		status:     "Connected to " + addr,
		history:    []string{},
		historyIdx: 0,
	}
}

func (m model) Init() tea.Cmd {
	return textarea.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height

		headerHeight := 1
		statusHeight := 1
		helpHeight := 1
		inputHeight := 5

		viewportHeight := m.height - headerHeight - statusHeight - helpHeight - inputHeight - 2
		if viewportHeight < 5 {
			viewportHeight = 5
		}

		m.viewport.Width = m.width - 4
		m.viewport.Height = viewportHeight

		m.input.SetWidth(m.width - 4)

		m.help.Width = m.width

	case tea.KeyMsg:
		if key.Matches(msg, m.keys.Quit) {
			return m, tea.Quit
		}

		if key.Matches(msg, m.keys.Clear) {
			m.viewport.SetContent("Cleared.")
			m.viewport.GotoBottom()
			return m, nil
		}

		if key.Matches(msg, m.keys.HistoryUp) {
			if len(m.history) > 0 {
				if m.historyIdx > 0 {
					m.historyIdx--
				}
				m.input.SetValue(m.history[m.historyIdx])
				m.input.CursorEnd()
			}
			return m, nil
		}

		if key.Matches(msg, m.keys.HistoryDown) {
			if len(m.history) > 0 {
				if m.historyIdx < len(m.history)-1 {
					m.historyIdx++
					m.input.SetValue(m.history[m.historyIdx])
				} else {
					m.historyIdx = len(m.history)
					m.input.SetValue("")
				}
				m.input.CursorEnd()
			}
			return m, nil
		}

		if key.Matches(msg, m.keys.Run) {
			line := strings.TrimSpace(m.input.Value())
			if line == "" {
				break
			}

			// Add to history
			if len(m.history) == 0 || m.history[len(m.history)-1] != line {
				m.history = append(m.history, line)
			}
			m.historyIdx = len(m.history)

			m.loading = true
			m.status = "Executing..."
			m.err = nil

			m.input.Reset()

			cmds = append(cmds, execSQLCmd(m.addr, line))
			return m, tea.Batch(cmds...)
		}

	case execMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			m.status = "Error"
			content := m.viewport.View() + "\n\n" +
				commandStyle.Render("> "+msg.sql) + "\n" +
				errorMessageStyle.Render(msg.err.Error())
			m.viewport.SetContent(content)
		} else {
			m.err = nil
			m.status = "Ready"
			content := m.viewport.View() + "\n\n" +
				commandStyle.Render("> "+msg.sql) + "\n" +
				msg.output
			m.viewport.SetContent(content)
		}
		m.viewport.GotoBottom()
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	cmds = append(cmds, cmd)
	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	// Header
	header := titleStyle.Render("LiminalDB TUI")

	// Viewport
	vp := viewportStyle.Render(m.viewport.View())

	// Status
	statusText := m.status
	if m.loading {
		statusText += " ⏳"
	}

	var status string
	if m.err != nil {
		status = statusErrorStyle.Width(m.width).Render("Error: " + m.err.Error())
	} else if m.loading {
		status = statusLoadingStyle.Width(m.width).Render(statusText)
	} else {
		status = statusOKStyle.Width(m.width).Render(statusText)
	}

	// Input
	inp := inputStyle.Render(m.input.View())

	// Help
	helpView := m.help.View(m.keys)

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		vp,
		status,
		inp,
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
