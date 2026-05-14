from datetime import datetime

def decode_safe(raw_bytes: bytes) -> str:
    """Decodifica bytes probando UTF-8 → Latin-1 → CP1252"""
    for encoding in ["utf-8", "latin-1", "cp1252"]:
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="ignore")

def parse_qr(raw: str) -> dict:
    """Parsea el payload del QR"""
    # Limpiar caracteres de control
    clean = "".join(c for c in raw if c.isprintable() and c not in "\r\n\t")
    
    if "|" not in clean or "=" not in clean:
        raise ValueError("Formato QR inválido")
    
    data = {}
    for part in clean.split("|"):
        if "=" in part:
            key, value = part.split("=", 1)
            data[key.strip()] = value.strip()
    
    # Validar campos requeridos
    required = ["NS", "PRD", "DSC", "LOT", "FEC", "VTO"]
    missing = [k for k in required if k not in data or not data[k]]
    if missing:
        raise ValueError(f"Faltan campos: {', '.join(missing)}")
    
    def to_int(s):
        try:
            return int(float(s.strip()))
        except:
            raise ValueError(f"Valor numérico inválido: {s}")
    
    def parse_date(s):
        s = s.strip()
        if "/" in s:
            try:
                return datetime.strptime(s, "%d/%m/%y").date().isoformat()
            except:
                return s
        return s
    
    return {
        "nro_serie": to_int(data["NS"]),
        "id_producto": to_int(data["PRD"]),
        "descripcion": data["DSC"].strip(),
        "lote": data["LOT"].strip(),
        "creacion": parse_date(data["FEC"]),
        "vencimiento": parse_date(data["VTO"]),
    }