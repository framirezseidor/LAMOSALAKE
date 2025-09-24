# -*- coding: utf-8 -*-
"""
Clona una estructura de carpetas desde un origen a un destino,
limpia el destino y reemplaza texto en archivos de texto.
- Omite archivos/carpetas ocultas o de sistema en Windows y temporales comunes.
- Omite .bak (solicitado).
- Reporta insights detallados al final.
"""

import os
import shutil
from pathlib import Path
import time

# === CONFIGURA AQUÍ ===
SOURCE_DIR = r"C:\\Users\\FernandoCuellar\\grupolamosa\\alteryx macros - Macros"
TARGET_DIR = r"C:\\Users\\FernandoCuellar\\Documents\\[1] Seidor\\[1] Proyectos\\LAMOSA DW S4\\Alteryx\\Macros"

SEARCH_TEXT  = "@ S4H QA"        # <--- lo que buscas
REPLACE_TEXT = "@ S4H QA 400"    # <--- por lo que lo reemplazas

TEXT_EXTENSIONS = {
    ".yxmc", ".yxmd", ".yxwz", ".yxwv",  # Alteryx (XML)
    ".xml", ".txt", ".csv", ".json", ".ini", ".cfg", ".md",
    ".bat", ".cmd", ".ps1", ".py", ".sql", ".yml", ".yaml"
}

# Carpetas/archivos a omitir explícitamente
SKIP_DIRS = {".git", ".svn", "__pycache__"}
SKIP_FILES_EXACT = {"Thumbs.db", "desktop.ini"}
SKIP_FILES_PREFIX = {"~$"}              # p. ej., temporales de Office
SKIP_FILES_SUFFIX = {".bak"}            # <--- omitir .bak

# ===== Utilidades =====
def _is_hidden_or_system(path: Path) -> bool:
    if os.name != "nt":
        return False
    try:
        import ctypes
        FILE_ATTRIBUTE_HIDDEN = 0x2
        FILE_ATTRIBUTE_SYSTEM = 0x4
        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
        if attrs == -1:
            return False
        return bool(attrs & (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM))
    except Exception:
        return False

def _skip_file(name: str) -> bool:
    lname = name.lower()
    if name in SKIP_FILES_EXACT:
        return True
    if any(lname.startswith(p.lower()) for p in SKIP_FILES_PREFIX):
        return True
    if any(lname.endswith(s.lower()) for s in SKIP_FILES_SUFFIX):
        return True
    if name.startswith("."):
        return True
    return False

def is_text_file(path: Path) -> bool:
    if path.suffix.lower() in TEXT_EXTENSIONS:
        return True
    # Heurística: intenta decodificar un trozo como UTF-8
    try:
        with path.open("rb") as f:
            sample = f.read(4096)
        sample.decode("utf-8")
        return True
    except Exception:
        return False

def clean_target(target: Path):
    if target.exists():
        print(f"Eliminando destino: {target}")
        shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True, exist_ok=True)

def _fmt_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024.0

def clone_with_replace(source: Path, target: Path):
    files_copied = 0
    files_replaced = 0          # archivos en los que hubo intento de reemplazo
    replaced_occurrences = 0    # # total de ocurrencias reemplazadas
    skipped = 0
    skipped_hidden_or_system = 0
    errors = 0
    total_bytes = 0
    dirs_created = 0
    per_ext_counts = {}         # ".xml": 10, ".py": 3, ...
    top_largest = []            # [(size, dst_path), ...] mantén top 5

    def record_copy(dst_path: Path):
        nonlocal total_bytes, top_largest
        try:
            size = dst_path.stat().st_size
            total_bytes += size
            # Mantener top 5 más grandes
            top_largest.append((size, dst_path))
            top_largest = sorted(top_largest, key=lambda x: x[0], reverse=True)[:5]
        except Exception:
            pass

    for root, dirs, files in os.walk(source):
        root_path = Path(root)

        # Omitir directorios ocultos/sistema y los de SKIP_DIRS
        filtered_dirs = []
        for d in dirs:
            if d in SKIP_DIRS or d.startswith(".") or _is_hidden_or_system(root_path / d):
                continue
            filtered_dirs.append(d)
        dirs[:] = filtered_dirs

        rel = os.path.relpath(root, source)
        target_root = target if rel == "." else target / rel
        if not target_root.exists():
            target_root.mkdir(parents=True, exist_ok=True)
            dirs_created += 1

        for name in files:
            src_path = root_path / name

            # Saltar archivos ocultos/sistema
            if _is_hidden_or_system(src_path):
                skipped_hidden_or_system += 1
                continue
            # Saltar por reglas custom
            if _skip_file(name):
                skipped += 1
                continue

            dst_path = target_root / name
            try:
                ext = src_path.suffix.lower()
                if is_text_file(src_path):
                    try:
                        raw = src_path.read_text(encoding="utf-8", errors="replace")
                        occ = 0
                        if SEARCH_TEXT:
                            occ = raw.count(SEARCH_TEXT)
                            if occ > 0:
                                raw = raw.replace(SEARCH_TEXT, REPLACE_TEXT)
                                files_replaced += 1
                                replaced_occurrences += occ
                        dst_path.write_text(raw, encoding="utf-8", newline="")
                        record_copy(dst_path)
                    except Exception as e:
                        print(f"[AVISO] Texto falló, copio binario: {src_path} -> {e}")
                        shutil.copy2(src_path, dst_path)
                        record_copy(dst_path)
                    files_copied += 1
                else:
                    shutil.copy2(src_path, dst_path)
                    record_copy(dst_path)
                    files_copied += 1

                per_ext_counts[ext] = per_ext_counts.get(ext, 0) + 1

            except PermissionError as e:
                errors += 1
                print(f"[PERMISO] Omitido por permisos: {src_path} -> {e}")
                continue
            except FileNotFoundError as e:
                errors += 1
                print(f"[NO ENCONTRADO] Omitido: {src_path} -> {e}")
                continue
            except OSError as e:
                errors += 1
                print(f"[OSERROR] Omitido: {src_path} -> {e}")
                continue

    # ---- Resumen ----
    print("\n=== RESUMEN ===")
    print(f"Copiados: {files_copied}")
    print(f"Archivos con reemplazo: {files_replaced}")
    print(f"Ocurrencias reemplazadas: {replaced_occurrences}")
    print(f"Omitidos (reglas .bak/temporal/.prefijo): {skipped}")
    print(f"Omitidos (ocultos/sistema): {skipped_hidden_or_system}")
    print(f"Errores: {errors}")
    print(f"Tamaño total copiado: {_fmt_size(total_bytes)}")
    print(f"Carpetas creadas: {dirs_created}")
    if per_ext_counts:
        top_ext = sorted(per_ext_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        printable = ", ".join([f"{ext or '(sin ext)'}: {cnt}" for ext, cnt in top_ext])
        print(f"Top extensiones copiadas: {printable}")
    if top_largest:
        print("Top 5 archivos más grandes copiados:")
        for size, path in top_largest:
            print(f"  - {_fmt_size(size)}  |  {path}")

def main():
    start = time.monotonic()

    src = Path(SOURCE_DIR)
    dst = Path(TARGET_DIR)

    if not src.exists():
        raise FileNotFoundError(f"No se encontró el origen: {src}")

    print("=== Clonado con replace ===")
    print(f"Origen : {src}")
    print(f"Destino: {dst}")
    print(f"Buscar : {SEARCH_TEXT!r}")
    print(f"Reempl.: {REPLACE_TEXT!r}")
    print("===========================")

    clean_target(dst)
    clone_with_replace(src, dst)

    elapsed = time.monotonic() - start
    print(f"Tiempo total: {elapsed:.2f} s")

if __name__ == "__main__":
    main()
