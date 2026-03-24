/**
 * XxScript Syntax Highlighter
 * A simple syntax highlighter for XxScript code
 */

(function(global) {
    'use strict';

    // Keywords to highlight
    const KEYWORDS = [
        'var', 'let', 'const', 'function', 'if', 'else', 'for', 'while', 'return',
        'break', 'continue', 'switch', 'case', 'default', 'try', 'catch', 'finally',
        'throw', 'new', 'delete', 'typeof', 'instanceof', 'in', 'of',
        'true', 'false', 'null', 'undefined', 'this', 'class', 'extends'
    ];

    // Built-in objects/functions
    const BUILTINS = [
        'http', 'db', 'request', 'response', 'console', 'JSON', 'Math', 'Date',
        'String', 'Number', 'Array', 'Object', 'Boolean', 'parseInt', 'parseFloat',
        'isNaN', 'isFinite', 'encodeURI', 'decodeURI', 'encodeURIComponent', 'decodeURIComponent',
        'fileSave', 'fileRead', 'fileAppend', 'fileDelete', 'fileExists',
        'dirCreate', 'dirDelete', 'dirList', 'dirExists',
        'log', 'println', 'sprintf', 'md5', 'sha256', 'base64Encode', 'base64Decode',
        'jsonEncode', 'jsonDecode', 'htmlEscape', 'urlEncode', 'urlDecode',
        'projectRegister', 'projectUnregister', 'sleep'
    ];

    // Token types and their CSS classes
    const TOKEN_TYPES = {
        KEYWORD: 'token-keyword',
        BUILTIN: 'token-builtin',
        STRING: 'token-string',
        NUMBER: 'token-number',
        COMMENT: 'token-comment',
        OPERATOR: 'token-operator',
        PUNCTUATION: 'token-punctuation',
        FUNCTION: 'token-function',
        IDENTIFIER: 'token-identifier'
    };

    // Escape HTML special characters
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Check if a character is a digit
    function isDigit(char) {
        return char >= '0' && char <= '9';
    }

    // Check if a character is a letter or underscore
    function isLetter(char) {
        return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char === '_';
    }

    // Check if a character is a word character
    function isWordChar(char) {
        return isLetter(char) || isDigit(char);
    }

    // Check if a character is an operator
    function isOperator(char) {
        return '+-*/%=<>!&|^~?:'.indexOf(char) !== -1;
    }

    // Check if a character is punctuation
    function isPunctuation(char) {
        return '(){}[];,.'.indexOf(char) !== -1;
    }

    // Check if a character is whitespace
    function isWhitespace(char) {
        return char === ' ' || char === '\t' || char === '\n' || char === '\r';
    }

    // Tokenize the input code
    function tokenize(code) {
        const tokens = [];
        let i = 0;
        const len = code.length;

        while (i < len) {
            const char = code[i];

            // Whitespace
            if (isWhitespace(char)) {
                let ws = '';
                while (i < len && isWhitespace(code[i])) {
                    ws += code[i];
                    i++;
                }
                tokens.push({ type: 'WHITESPACE', value: ws });
                continue;
            }

            // Single-line comment
            if (char === '/' && i + 1 < len && code[i + 1] === '/') {
                let comment = '';
                while (i < len && code[i] !== '\n') {
                    comment += code[i];
                    i++;
                }
                tokens.push({ type: 'COMMENT', value: comment });
                continue;
            }

            // Multi-line comment
            if (char === '/' && i + 1 < len && code[i + 1] === '*') {
                let comment = '/*';
                i += 2;
                while (i < len - 1 && !(code[i] === '*' && code[i + 1] === '/')) {
                    comment += code[i];
                    i++;
                }
                if (i < len - 1) {
                    comment += '*/';
                    i += 2;
                }
                tokens.push({ type: 'COMMENT', value: comment });
                continue;
            }

            // String literal (double quote)
            if (char === '"') {
                let str = '"';
                i++;
                while (i < len && code[i] !== '"') {
                    if (code[i] === '\\' && i + 1 < len) {
                        str += code[i] + code[i + 1];
                        i += 2;
                    } else {
                        str += code[i];
                        i++;
                    }
                }
                if (i < len) {
                    str += '"';
                    i++;
                }
                tokens.push({ type: 'STRING', value: str });
                continue;
            }

            // String literal (single quote)
            if (char === "'") {
                let str = "'";
                i++;
                while (i < len && code[i] !== "'") {
                    if (code[i] === '\\' && i + 1 < len) {
                        str += code[i] + code[i + 1];
                        i += 2;
                    } else {
                        str += code[i];
                        i++;
                    }
                }
                if (i < len) {
                    str += "'";
                    i++;
                }
                tokens.push({ type: 'STRING', value: str });
                continue;
            }

            // Template literal (backtick)
            if (char === '`') {
                let str = '`';
                i++;
                while (i < len && code[i] !== '`') {
                    if (code[i] === '\\' && i + 1 < len) {
                        str += code[i] + code[i + 1];
                        i += 2;
                    } else {
                        str += code[i];
                        i++;
                    }
                }
                if (i < len) {
                    str += '`';
                    i++;
                }
                tokens.push({ type: 'STRING', value: str });
                continue;
            }

            // Number
            if (isDigit(char) || (char === '.' && i + 1 < len && isDigit(code[i + 1]))) {
                let num = '';
                // Integer part
                while (i < len && isDigit(code[i])) {
                    num += code[i];
                    i++;
                }
                // Decimal part
                if (i < len && code[i] === '.') {
                    num += '.';
                    i++;
                    while (i < len && isDigit(code[i])) {
                        num += code[i];
                        i++;
                    }
                }
                // Exponent
                if (i < len && (code[i] === 'e' || code[i] === 'E')) {
                    num += code[i];
                    i++;
                    if (i < len && (code[i] === '+' || code[i] === '-')) {
                        num += code[i];
                        i++;
                    }
                    while (i < len && isDigit(code[i])) {
                        num += code[i];
                        i++;
                    }
                }
                tokens.push({ type: 'NUMBER', value: num });
                continue;
            }

            // Identifier or keyword
            if (isLetter(char)) {
                let id = '';
                while (i < len && isWordChar(code[i])) {
                    id += code[i];
                    i++;
                }

                // Check if it's a keyword
                if (KEYWORDS.indexOf(id) !== -1) {
                    tokens.push({ type: 'KEYWORD', value: id });
                } else if (BUILTINS.indexOf(id) !== -1) {
                    tokens.push({ type: 'BUILTIN', value: id });
                } else {
                    // Check if it's a function call
                    let j = i;
                    while (j < len && isWhitespace(code[j])) j++;
                    if (j < len && code[j] === '(') {
                        tokens.push({ type: 'FUNCTION', value: id });
                    } else {
                        tokens.push({ type: 'IDENTIFIER', value: id });
                    }
                }
                continue;
            }

            // Operators
            if (isOperator(char)) {
                let op = '';
                // Handle multi-character operators
                while (i < len && isOperator(code[i])) {
                    op += code[i];
                    i++;
                }
                tokens.push({ type: 'OPERATOR', value: op });
                continue;
            }

            // Punctuation
            if (isPunctuation(char)) {
                tokens.push({ type: 'PUNCTUATION', value: char });
                i++;
                continue;
            }

            // Unknown character - just add it
            tokens.push({ type: 'UNKNOWN', value: char });
            i++;
        }

        return tokens;
    }

    // Convert tokens to highlighted HTML
    function tokensToHtml(tokens) {
        let html = '';

        for (const token of tokens) {
            const escaped = escapeHtml(token.value);

            switch (token.type) {
                case 'KEYWORD':
                    html += '<span class="' + TOKEN_TYPES.KEYWORD + '">' + escaped + '</span>';
                    break;
                case 'BUILTIN':
                    html += '<span class="' + TOKEN_TYPES.BUILTIN + '">' + escaped + '</span>';
                    break;
                case 'STRING':
                    html += '<span class="' + TOKEN_TYPES.STRING + '">' + escaped + '</span>';
                    break;
                case 'NUMBER':
                    html += '<span class="' + TOKEN_TYPES.NUMBER + '">' + escaped + '</span>';
                    break;
                case 'COMMENT':
                    html += '<span class="' + TOKEN_TYPES.COMMENT + '">' + escaped + '</span>';
                    break;
                case 'FUNCTION':
                    html += '<span class="' + TOKEN_TYPES.FUNCTION + '">' + escaped + '</span>';
                    break;
                case 'OPERATOR':
                    html += '<span class="' + TOKEN_TYPES.OPERATOR + '">' + escaped + '</span>';
                    break;
                case 'PUNCTUATION':
                    html += '<span class="' + TOKEN_TYPES.PUNCTUATION + '">' + escaped + '</span>';
                    break;
                case 'WHITESPACE':
                case 'UNKNOWN':
                case 'IDENTIFIER':
                default:
                    html += escaped;
                    break;
            }
        }

        return html;
    }

    /**
     * Highlight XxScript code
     * @param {string} code - The code to highlight
     * @returns {string} - HTML with syntax highlighting
     */
    function highlight(code) {
        const tokens = tokenize(code);
        return tokensToHtml(tokens);
    }

    /**
     * Create an editable code editor with syntax highlighting
     * @param {HTMLElement} textarea - The textarea to enhance
     * @param {Object} options - Configuration options
     */
    function createEditor(textarea, options) {
        options = options || {};

        // Create container
        const container = document.createElement('div');
        container.className = 'xxscript-editor-container';
        container.style.cssText = 'position: relative; overflow: hidden;';

        // Create the highlight overlay
        const overlay = document.createElement('div');
        overlay.className = 'xxscript-highlight-overlay';
        overlay.style.cssText = `
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            white-space: pre-wrap;
            word-wrap: break-word;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 14px;
            line-height: 1.5;
            padding: 15px;
            margin: 0;
            border: 1px solid transparent;
            border-radius: 4px;
            color: transparent;
        `;

        // Style the textarea
        textarea.style.cssText = `
            position: relative;
            z-index: 1;
            width: 100%;
            min-height: 300px;
            font-family: 'Courier New', monospace;
            font-size: 14px;
            line-height: 1.5;
            padding: 15px;
            margin: 0;
            background: transparent;
            color: #333;
            caret-color: #333;
            resize: vertical;
            border: 1px solid #ddd;
            border-radius: 4px;
            tab-size: 4;
        `;

        // Insert container before textarea
        textarea.parentNode.insertBefore(container, textarea);
        container.appendChild(overlay);
        container.appendChild(textarea);

        // Update function
        function update() {
            overlay.innerHTML = highlight(textarea.value);
        }

        // Sync scroll
        textarea.addEventListener('scroll', function() {
            overlay.scrollTop = textarea.scrollTop;
            overlay.scrollLeft = textarea.scrollLeft;
        });

        // Handle input
        textarea.addEventListener('input', update);

        // Handle tab key
        textarea.addEventListener('keydown', function(e) {
            if (e.key === 'Tab') {
                e.preventDefault();
                const start = textarea.selectionStart;
                const end = textarea.selectionEnd;
                const value = textarea.value;
                textarea.value = value.substring(0, start) + '    ' + value.substring(end);
                textarea.selectionStart = textarea.selectionEnd = start + 4;
                update();
            }
        });

        // Initial highlight
        update();

        return {
            update: update,
            getValue: function() { return textarea.value; },
            setValue: function(code) {
                textarea.value = code;
                update();
            }
        };
    }

    // Add CSS styles for syntax highlighting
    function addStyles() {
        if (document.getElementById('xxscript-styles')) return;

        const style = document.createElement('style');
        style.id = 'xxscript-styles';
        style.textContent = `
            .xxscript-highlight-overlay .token-keyword {
                color: #0000ff;
                font-weight: bold;
            }
            .xxscript-highlight-overlay .token-builtin {
                color: #0086b3;
            }
            .xxscript-highlight-overlay .token-string {
                color: #d14;
            }
            .xxscript-highlight-overlay .token-number {
                color: #099;
            }
            .xxscript-highlight-overlay .token-comment {
                color: #998;
                font-style: italic;
            }
            .xxscript-highlight-overlay .token-function {
                color: #990000;
                font-weight: bold;
            }
            .xxscript-highlight-overlay .token-operator {
                color: #333;
            }
            .xxscript-highlight-overlay .token-punctuation {
                color: #333;
            }

            /* Dark theme support */
            .dark-theme .xxscript-highlight-overlay .token-keyword {
                color: #569cd6;
            }
            .dark-theme .xxscript-highlight-overlay .token-builtin {
                color: #4ec9b0;
            }
            .dark-theme .xxscript-highlight-overlay .token-string {
                color: #ce9178;
            }
            .dark-theme .xxscript-highlight-overlay .token-number {
                color: #b5cea8;
            }
            .dark-theme .xxscript-highlight-overlay .token-comment {
                color: #6a9955;
            }
            .dark-theme .xxscript-highlight-overlay .token-function {
                color: #dcdcaa;
            }
            .dark-theme .xxscript-highlight-overlay .token-operator {
                color: #d4d4d4;
            }
            .dark-theme .xxscript-highlight-overlay .token-punctuation {
                color: #d4d4d4;
            }
        `;
        document.head.appendChild(style);
    }

    // Initialize styles
    addStyles();

    // Export to global
    global.XxScriptHighlight = {
        highlight: highlight,
        createEditor: createEditor,
        tokenize: tokenize
    };

})(typeof window !== 'undefined' ? window : this);