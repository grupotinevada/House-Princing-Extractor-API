import os
import ast
from importlib.metadata import distributions

PROJECT_PATH = "."

SPECIAL_CASES = {
    "cv2": "opencv-python",
    "PIL": "Pillow",
    "sklearn": "scikit-learn",
    "yaml": "PyYAML",
    "Crypto": "pycryptodome",
    "bs4": "beautifulsoup4",
}

def get_top_level_packages():
    """
    Mapea imports reales → paquete pip
    """
    mapping = {}

    for dist in distributions():
        name = dist.metadata["Name"]

        try:
            top_level = dist.read_text("top_level.txt")
            if top_level:
                for line in top_level.splitlines():
                    mapping[line.strip()] = name
        except Exception:
            pass

    return mapping


def get_imports_from_file(file_path):
    imports = set()
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=file_path)
    except Exception:
        return imports

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                imports.add(name.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split(".")[0])

    return imports


def scan_project(path):
    all_imports = set()

    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d not in ("venv", "__pycache__", ".git", "node_modules")]

        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                all_imports.update(get_imports_from_file(file_path))

    return all_imports


def get_local_modules(path):
    """
    Detecta módulos propios del proyecto
    """
    local_modules = set()

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".py"):
                local_modules.add(file.replace(".py", ""))

    return local_modules


def main():
    print("🔍 Escaneando proyecto...")

    imports = scan_project(PROJECT_PATH)
    local_modules = get_local_modules(PROJECT_PATH)
    mapping = get_top_level_packages()

    requirements = {}

    for imp in imports:
        if imp in local_modules:
            continue

        pkg_name = SPECIAL_CASES.get(imp)

        if not pkg_name:
            pkg_name = mapping.get(imp)

        if not pkg_name:
            continue  # ignorar basura automáticamente

        # obtener versión
        for dist in distributions():
            if dist.metadata["Name"] == pkg_name:
                requirements[pkg_name] = dist.version

    with open("requirements.txt", "w", encoding="utf-8") as f:
        for pkg, version in sorted(requirements.items()):
            f.write(f"{pkg}=={version}\n")

    print("✅ requirements.txt limpio generado")
    print(f"📦 Total dependencias reales: {len(requirements)}")


if __name__ == "__main__":
    main()