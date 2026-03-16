package web

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/topxeq/xxsql/internal/auth"
)

// Session duration
const sessionDuration = 24 * time.Hour

// handleAPILogin handles POST /api/login.
func (s *Server) handleAPILogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	// Authenticate
	session, err := s.auth.Authenticate(req.Username, req.Password)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	// Create web session
	webSession := &Session{
		ID:       generateSessionID(),
		Username: req.Username,
		Created:  time.Now(),
		Expires:  time.Now().Add(sessionDuration),
	}
	s.sessions[webSession.ID] = webSession

	// Set cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "xxsql_session",
		Value:    webSession.ID,
		Path:     "/",
		Expires:  webSession.Expires,
		HttpOnly: true,
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":  "logged in",
		"username": req.Username,
		"role":     session.Role,
	})
}

// handleAPILogout handles POST /api/logout.
func (s *Server) handleAPILogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Get session cookie
	cookie, err := r.Cookie("xxsql_session")
	if err == nil {
		delete(s.sessions, cookie.Value)
	}

	// Clear cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "xxsql_session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
	})

	writeJSON(w, http.StatusOK, map[string]string{"message": "logged out"})
}

// getSession returns the current session from the request.
func (s *Server) getSession(r *http.Request) *Session {
	cookie, err := r.Cookie("xxsql_session")
	if err != nil {
		return nil
	}

	session, exists := s.sessions[cookie.Value]
	if !exists {
		return nil
	}

	// Check expiration
	if time.Now().After(session.Expires) {
		delete(s.sessions, cookie.Value)
		return nil
	}

	return session
}

// generateSessionID generates a random session ID.
func generateSessionID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// hasPermission checks if the session user has a specific permission.
func (s *Server) hasPermission(r *http.Request, perm auth.Permission) bool {
	session := s.getSession(r)
	if session == nil {
		return false
	}

	has, err := s.auth.CheckPermission(session.Username, perm)
	return err == nil && has
}
