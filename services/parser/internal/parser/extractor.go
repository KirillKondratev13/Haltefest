package parser

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	pdf "github.com/ledongthuc/pdf"
	"github.com/lu4p/cat"
	"github.com/pemistahl/lingua-go"
)

type Extractor struct {
	langDetector lingua.LanguageDetector
}

func NewExtractor() *Extractor {
	detector := lingua.NewLanguageDetectorBuilder().
		FromAllLanguages().
		WithLowAccuracyMode().
		Build()
	
	return &Extractor{
		langDetector: detector,
	}
}

func (e *Extractor) Extract(data []byte, mimeType string) (string, error) {
	var text string
	var err error

	switch mimeType {
	case "application/pdf":
		text, err = e.extractPDF(data)
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		text, err = e.extractDOCX(data)
	default:
		if strings.HasPrefix(mimeType, "text/plain") {
			text = string(data)
		} else {
			return "", fmt.Errorf("unsupported mime type: %s", mimeType)
		}
	}

	if err != nil {
		return "", err
	}

	text = cleanText(text)
	return strings.TrimSpace(text), nil
}

func (e *Extractor) IsEnglish(text string) (bool, string) {
	sample := text
	if len(sample) > 10000 {
		sample = sample[:10000]
	}
	
	// Детектируем язык (логика сохранена для логирования/статистики)
	detectedLang, ok := e.langDetector.DetectLanguageOf(sample)
	if !ok {
		// ЗАГЛУШКА: пропускаем даже если язык не определён
		return true, "unknown"
	}
	
	// ЗАГЛУШКА: всегда возвращаем true, любой язык проходит
	_ = detectedLang.IsoCode639_1() // явно используем, чтобы не было warning
	
	return true, detectedLang.String()
}

func (e *Extractor) extractPDF(data []byte) (string, error) {
	reader := bytes.NewReader(data)
	
	r, err := pdf.NewReader(reader, int64(len(data)))
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

		text, err := page.GetPlainText(nil)
		if err != nil {
			continue
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
	text, err := cat.FromBytes(data)
	if err != nil {
		return "", fmt.Errorf("failed to extract text from docx: %w", err)
	}

	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("no text content found in docx")
	}

	return text, nil
}

func cleanText(text string) string {
	// Удаляем Unicode control characters (категория C)
	controlCharsRegex := regexp.MustCompile(`[\p{C}]`)
	text = controlCharsRegex.ReplaceAllString(text, "")

	// Удаляем специфические проблемные символы
	problematicChars := []rune{
		'\u00A0', // non-breaking space
		'\u200B', // zero-width space
		'\u200C', // zero-width non-joiner
		'\u200D', // zero-width joiner
		'\uFEFF', // BOM / zero-width no-break space
		'\u2060', // word joiner
		'\u180E', // mongolian vowel separator
	}
	
	for _, char := range problematicChars {
		text = strings.ReplaceAll(text, string(char), " ")
	}

	text = normalizeWhitespace(text)
	text = regexp.MustCompile(`[ \t]+`).ReplaceAllString(text, " ")
	text = regexp.MustCompile(`\n\s*\n`).ReplaceAllString(text, "\n\n")

	return text
}

func normalizeWhitespace(text string) string {
	var sb strings.Builder
	lastWasSpace := false

	for _, r := range text {
		isSpace := r == ' ' || r == '\t' || r == '\n' || r == '\r'
		
		if isSpace {
			if !lastWasSpace {
				sb.WriteRune(' ')
			}
			lastWasSpace = true
		} else {
			if unicode.IsPrint(r) {
				sb.WriteRune(r)
			}
			lastWasSpace = false
		}
	}

	return sb.String()
}