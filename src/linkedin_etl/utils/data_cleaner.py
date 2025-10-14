def _sanitize(value):
    """Make values safe for line-based CSV loading.
    - Remove internal newlines and carriage returns.
    - Remove NULs.
    - Strip surrounding whitespace.
    - Leave quoting to csv module.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return value
    s = str(value)
    s = s.replace("\x00", "")
    s = s.replace("\r", " ").replace("\n", "<br>")
    return s.strip()

def prep_row(raw: dict, columns: list[str]) -> dict:
    return {col: _sanitize(raw.get(col)) for col in columns}