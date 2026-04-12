package xxscript

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch30_PDFExcelAndCharts(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinPDFAddPage([]Value{}).(map[string]Value); m["error"] != "need document ID" {
		t.Fatalf("expected pdfAddPage arg error, got %v", m)
	}
	if m := i.builtinPDFSetFont([]Value{}).(map[string]Value); m["error"] != "need document ID, font family, and size" {
		t.Fatalf("expected pdfSetFont arg error, got %v", m)
	}
	if m := i.builtinPDFAddText([]Value{}).(map[string]Value); m["error"] != "need document ID, x, y, and text" {
		t.Fatalf("expected pdfAddText arg error, got %v", m)
	}
	if m := i.builtinPDFCell([]Value{}).(map[string]Value); m["error"] != "need document ID, width, height, text, border" {
		t.Fatalf("expected pdfCell arg error, got %v", m)
	}
	if m := i.builtinPDFSave([]Value{}).(map[string]Value); m["error"] != "need document ID and file path" {
		t.Fatalf("expected pdfSave arg error, got %v", m)
	}
	if m := i.builtinPDFAddPage([]Value{"missing"}).(map[string]Value); m["error"] != "PDF document not found" {
		t.Fatalf("expected pdfAddPage missing-doc error, got %v", m)
	}

	pdfCreated := i.builtinPDFCreate([]Value{"P", "A4"}).(map[string]Value)
	pdfID, _ := pdfCreated["id"].(string)
	if pdfCreated["created"] != true || !strings.HasPrefix(pdfID, "pdf_") {
		t.Fatalf("expected pdfCreate success payload, got %v", pdfCreated)
	}
	if m := i.builtinPDFAddPage([]Value{pdfID}).(map[string]Value); m["added"] != true {
		t.Fatalf("expected pdfAddPage success, got %v", m)
	}
	if m := i.builtinPDFSetFont([]Value{pdfID, "Arial", 12.0, ""}).(map[string]Value); m["set"] != true {
		t.Fatalf("expected pdfSetFont success, got %v", m)
	}
	if m := i.builtinPDFAddText([]Value{pdfID, 10.0, 20.0, "hello"}).(map[string]Value); m["added"] != true {
		t.Fatalf("expected pdfAddText success, got %v", m)
	}
	if m := i.builtinPDFCell([]Value{pdfID, 40.0, 8.0, "c", 1, "C"}).(map[string]Value); m["added"] != true {
		t.Fatalf("expected pdfCell success, got %v", m)
	}
	pdfPath := filepath.Join(t.TempDir(), "x.pdf")
	pdfSaved := i.builtinPDFSave([]Value{pdfID, pdfPath}).(map[string]Value)
	if pdfSaved["saved"] != true || pdfSaved["path"] != pdfPath {
		t.Fatalf("expected pdfSave success payload, got %v", pdfSaved)
	}
	if st, err := os.Stat(pdfPath); err != nil || st.Size() == 0 {
		t.Fatalf("expected saved PDF file on disk, err=%v", err)
	}
	if m := i.builtinPDFAddPage([]Value{pdfID}).(map[string]Value); m["error"] != "PDF document not found" {
		t.Fatalf("expected pdf cleanup after save, got %v", m)
	}

	if m := i.builtinExcelOpen([]Value{}).(map[string]Value); m["error"] != "need file path" {
		t.Fatalf("expected excelOpen arg error, got %v", m)
	}
	if m := i.builtinExcelSetCell([]Value{}).(map[string]Value); m["error"] != "need document ID, sheet, cell, and value" {
		t.Fatalf("expected excelSetCell arg error, got %v", m)
	}
	if m := i.builtinExcelGetCell([]Value{}).(map[string]Value); m["error"] != "need document ID, sheet, and cell" {
		t.Fatalf("expected excelGetCell arg error, got %v", m)
	}
	if m := i.builtinExcelNewSheet([]Value{}).(map[string]Value); m["error"] != "need document ID and sheet name" {
		t.Fatalf("expected excelNewSheet arg error, got %v", m)
	}
	if m := i.builtinExcelSave([]Value{}).(map[string]Value); m["error"] != "need document ID and file path" {
		t.Fatalf("expected excelSave arg error, got %v", m)
	}
	if m := i.builtinExcelClose([]Value{}).(map[string]Value); m["error"] != "need document ID" {
		t.Fatalf("expected excelClose arg error, got %v", m)
	}
	if m := i.builtinExcelOpen([]Value{filepath.Join(t.TempDir(), "missing.xlsx")}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected excelOpen missing-file error, got %v", m)
	}

	excelCreated := i.builtinExcelCreate(nil).(map[string]Value)
	excelID, _ := excelCreated["id"].(string)
	if excelCreated["created"] != true || !strings.HasPrefix(excelID, "excel_") {
		t.Fatalf("expected excelCreate success payload, got %v", excelCreated)
	}
	if m := i.builtinExcelSetCell([]Value{excelID, "Sheet1", "A1", "hello"}).(map[string]Value); m["set"] != true {
		t.Fatalf("expected excelSetCell success, got %v", m)
	}
	if m := i.builtinExcelSetCell([]Value{excelID, "Sheet1", "B1", true}).(map[string]Value); m["set"] != true {
		t.Fatalf("expected excelSetCell bool success, got %v", m)
	}
	if m := i.builtinExcelGetCell([]Value{excelID, "Sheet1", "A1"}).(map[string]Value); m["found"] != true || m["value"] != "hello" {
		t.Fatalf("expected excelGetCell success for A1, got %v", m)
	}
	if m := i.builtinExcelNewSheet([]Value{excelID, "Data"}).(map[string]Value); m["created"] != true {
		t.Fatalf("expected excelNewSheet success, got %v", m)
	}
	xlsxPath := filepath.Join(t.TempDir(), "x.xlsx")
	if m := i.builtinExcelSave([]Value{excelID, xlsxPath}).(map[string]Value); m["saved"] != true {
		t.Fatalf("expected excelSave success, got %v", m)
	}
	if _, err := os.Stat(xlsxPath); err != nil {
		t.Fatalf("expected saved xlsx file on disk: %v", err)
	}
	if m := i.builtinExcelClose([]Value{excelID}).(map[string]Value); m["closed"] != true {
		t.Fatalf("expected excelClose success, got %v", m)
	}
	if m := i.builtinExcelGetCell([]Value{excelID, "Sheet1", "A1"}).(map[string]Value); m["error"] != "Excel document not found" {
		t.Fatalf("expected excel document missing after close, got %v", m)
	}

	if m := i.builtinChartLine([]Value{}).(map[string]Value); m["error"] != "need title and data array" {
		t.Fatalf("expected chartLine arg error, got %v", m)
	}
	if m := i.builtinChartLine([]Value{"t", []Value{}}).(map[string]Value); m["error"] != "no data points provided" {
		t.Fatalf("expected chartLine empty-data error, got %v", m)
	}
	if m := i.builtinChartBar([]Value{"t", []Value{}}).(map[string]Value); m["error"] != "no data points provided" {
		t.Fatalf("expected chartBar empty-data error, got %v", m)
	}
	if m := i.builtinChartPie([]Value{"t", []Value{}}).(map[string]Value); m["error"] != "no data points provided" {
		t.Fatalf("expected chartPie empty-data error, got %v", m)
	}

	points := []Value{
		map[string]Value{"label": "A", "value": 10},
		map[string]Value{"label": "B", "value": 20.0},
	}
	line := i.builtinChartLine([]Value{"Line", points}).(map[string]Value)
	if line["created"] != true || line["type"] != "line" || !strings.Contains(line["svg"].(string), "<svg") {
		t.Fatalf("expected chartLine success payload, got %v", line)
	}
	bar := i.builtinChartBar([]Value{"Bar", points}).(map[string]Value)
	if bar["created"] != true || bar["type"] != "bar" || !strings.Contains(bar["svg"].(string), "<rect") {
		t.Fatalf("expected chartBar success payload, got %v", bar)
	}
	pie := i.builtinChartPie([]Value{"Pie", points}).(map[string]Value)
	if pie["created"] != true || pie["type"] != "pie" || !strings.Contains(pie["svg"].(string), "<path") {
		t.Fatalf("expected chartPie success payload, got %v", pie)
	}

	if svg := generateLineChartSVG("One", []map[string]Value{{"label": "x", "value": 1}}); strings.Contains(svg, `<path d="M`) {
		t.Fatalf("expected one-point line chart without path segment, got %s", svg)
	}
	if svg := generateLineChartSVG("Two", []map[string]Value{{"label": "x", "value": 0}, {"label": "y", "value": 0}}); !strings.Contains(svg, `<path d="M`) {
		t.Fatalf("expected multi-point line chart path, got %s", svg)
	}
	if svg := generateBarChartSVG("B", []map[string]Value{{"label": "x", "value": 0}, {"label": "y", "value": 0}}); !strings.Contains(svg, "<rect") {
		t.Fatalf("expected bar chart rect elements, got %s", svg)
	}
	if svg := generatePieChartSVG("P", []map[string]Value{{"label": "big", "value": 90}, {"label": "small", "value": 10}}); !strings.Contains(svg, " A 150 150 0 1 1 ") {
		t.Fatalf("expected pie chart large-arc branch, got %s", svg)
	}
	if svg := generatePieChartSVG("Zero", []map[string]Value{{"label": "z", "value": 0}}); !strings.Contains(svg, "0.0%") {
		t.Fatalf("expected pie chart zero-total fallback output, got %s", svg)
	}
}
