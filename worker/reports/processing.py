def make_result(event):
    if event.get("type") != "ReportRequested" or event.get("version") != 1:
        raise ValueError("Unsupported request event")
    text = event["text"]
    if not isinstance(text, str) or not text.strip():
        raise ValueError("text must be a non-empty string")
    return {
        "type": "ReportCompleted",
        "version": 1,
        "reportId": event["reportId"],
        "wordCount": len(text.split()),
        "characterCount": len(text),
    }
