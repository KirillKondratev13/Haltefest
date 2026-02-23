package parser

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	"github.com/abadojack/whatlanggo"
	pdf "github.com/ledongthuc/pdf"
)

type Extractor struct{}

func NewExtractor() *Extractor {
	return &Extractor{}
}

func (e *Extractor) Extract(data []byte, mimeType string) (string, error) {
	var text string
	var err error

	switch mimeType {
	case "application/pdf":
		text, err = e.extractPDF(data)
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		text, err = e.extractDOCX(data)
	case "text/plain":
		text = string(data)
	default:
		return "", fmt.Errorf("unsupported mime type: %s", mimeType)
	}

	if err != nil {
		return "", err
	}

	text = normalizeWhitespace(text)
	return strings.TrimSpace(text), nil
}

func (e *Extractor) IsEnglish(text string) bool {
	info := whatlanggo.Detect(text)
	return info.Lang == whatlanggo.Eng
}

func (e *Extractor) extractPDF(data []byte) (string, error) {
	r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("failed to create pdf reader: %w", err)
	}

	numPages := r.NumPage()
	if numPages == 0 {
		return "", fmt.Errorf("pdf has no pages")
	}

	var sb strings.Builder

	for i := 1; i <= numPages; i++ {
		page := r.Page(i)
		if page.V.IsNull() {
			continue
		}

		// Извлекаем текст со страницы
		text, err := page.GetPlainText(nil)
		if err != nil {
			continue // Пропускаем проблемные страницы
		}

		sb.WriteString(text)
		sb.WriteString("\n")
	}

	if sb.Len() == 0 {
		return "", fmt.Errorf("no text found in pdf")
	}

	return sb.String(), nil
}

func (e *Extractor) extractDOCX(data []byte) (string, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("failed to open docx zip: %w", err)
	}

	var sb strings.Builder

	for _, f := range r.File {
		if f.Name == "word/document.xml" {
			rc, err := f.Open()
			if err != nil {
				return "", fmt.Errorf("failed to open document.xml: %w", err)
			}

			content, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return "", fmt.Errorf("failed to read document.xml: %w", err)
			}

			text, err := extractTextFromOOXML(string(content))
			if err != nil {
				return "", fmt.Errorf("failed to parse OOXML: %w", err)
			}

			sb.WriteString(text)
		}

		// Извлекаем текст из header/footer
		if strings.HasPrefix(f.Name, "word/header") || strings.HasPrefix(f.Name, "word/footer") {
			rc, err := f.Open()
			if err != nil {
				continue
			}

			content, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				continue
			}

			text, err := extractTextFromOOXML(string(content))
			if err == nil {
				sb.WriteString("\n")
				sb.WriteString(text)
			}
		}
	}

	if sb.Len() == 0 {
		return "", fmt.Errorf("no text content found in docx")
	}

	return sb.String(), nil
}

func extractTextFromOOXML(xmlContent string) (string, error) {
	decoder := xml.NewDecoder(strings.NewReader(xmlContent))
	var sb strings.Builder
	var currentText strings.Builder

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		switch t := token.(type) {
		case xml.CharData:
			text := strings.TrimSpace(string(t))
			if text != "" {
				if currentText.Len() > 0 {
					currentText.WriteString(" ")
				}
				currentText.WriteString(text)
			}
		case xml.EndElement:
			if t.Name.Local == "p" {
				if currentText.Len() > 0 {
					sb.WriteString(currentText.String())
					sb.WriteString("\n")
					currentText.Reset()
				}
			} else if t.Name.Local == "br" {
				sb.WriteString(currentText.String())
				sb.WriteString("\n")
				currentText.Reset()
			} else if t.Name.Local == "tab" {
				currentText.WriteString("\t")
			}
		}
	}

	return sb.String(), nil
}

func normalizeWhitespace(text string) string {
	var sb strings.Builder
	lastWasSpace := false

	for _, r := range text {
		isSpace := r == ' ' || r == '\t' || r == '\n'
		if isSpace {
			if !lastWasSpace {
				sb.WriteRune(' ')
			}
			lastWasSpace = true
		} else {
			sb.WriteRune(r)
			lastWasSpace = false
		}
	}

	return sb.String()
}