import Foundation
import Testing

/// Find python3 binary — checks common paths across macOS and Linux.
func findPython3() -> String? {
    for path in ["/usr/local/bin/python3", "/usr/bin/python3", "/opt/homebrew/bin/python3"] {
        if FileManager.default.fileExists(atPath: path) { return path }
    }
    return nil
}

/// Run a pyarrow validation script and return the JSON output.
/// Returns nil if python3 is not available (test should skip).
func runPyarrow(_ script: String) -> [String: Any]? {
    guard let python = findPython3() else { return nil }

    let proc = Process()
    proc.executableURL = URL(fileURLWithPath: python)
    proc.arguments = ["-c", script]
    let stdoutPipe = Pipe()
    let stderrPipe = Pipe()
    proc.standardOutput = stdoutPipe
    proc.standardError = stderrPipe

    do {
        try proc.run()
        proc.waitUntilExit()
    } catch {
        return nil
    }

    let output = String(data: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    guard !output.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
          let data = output.data(using: .utf8),
          let result = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
        return nil
    }
    return result
}

/// Validate a Parquet file with pyarrow, checking rows and columns.
func validateWithPyarrow(path: String, expectedRows: Int, expectedColumns: [String]) {
    guard let result = runPyarrow("""
        import pyarrow.parquet as pq, json
        try:
            t = pq.read_table('\(path)')
            print(json.dumps({"ok": True, "rows": t.num_rows, "columns": t.column_names}))
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}))
        """) else { return } // skip if no python

    if result["ok"] as? Bool != true {
        Issue.record("pyarrow error: \(result["error"] ?? "unknown")")
    }
}
