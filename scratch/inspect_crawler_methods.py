import ast

with open("/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/data/osint_crawler.py", "r", encoding="utf-8") as f:
    tree = ast.parse(f.read())

for node in ast.walk(tree):
    if isinstance(node, ast.ClassDef) and node.name == "AresOsintCrawler":
        print(f"Class: {node.name}")
        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                doc = ast.get_docstring(item)
                first_line = doc.split("\n")[0] if doc else "No docstring"
                print(f"  Method: {item.name}(...) - {first_line}")
