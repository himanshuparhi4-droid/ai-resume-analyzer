from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
BACKEND = ROOT / "backend"
FRONTEND = ROOT / "frontend"


def ok(message: str) -> None:
    print(f"[OK] {message}")


def fail(message: str) -> None:
    print(f"[FAIL] {message}")


def check_path(path: Path, label: str) -> bool:
    if path.exists():
        ok(label)
        return True
    fail(f"{label} missing: {path}")
    return False


def check_frontend_config() -> bool:
    passed = True
    passed &= check_path(FRONTEND / "package.json", "frontend package.json exists")
    passed &= check_path(FRONTEND / "package-lock.json", "frontend package-lock.json exists")
    passed &= check_path(FRONTEND / ".env", "frontend local .env exists")
    passed &= check_path(FRONTEND / "vite.config.ts", "frontend Vite config exists")
    passed &= check_path(FRONTEND / "tailwind.config.ts", "frontend Tailwind config exists")
    passed &= check_path(FRONTEND / "src" / "main.tsx", "frontend entrypoint exists")

    frontend_env_path = FRONTEND / ".env"
    if frontend_env_path.exists():
        frontend_env = frontend_env_path.read_text(encoding="utf-8")
        if "VITE_API_BASE_URL=" in frontend_env:
            ok("frontend VITE_API_BASE_URL is configured")
        else:
            fail("frontend VITE_API_BASE_URL is missing")
            passed = False

    package_path = FRONTEND / "package.json"
    if package_path.exists():
        try:
            package = json.loads(package_path.read_text(encoding="utf-8"))
            scripts = package.get("scripts", {})
            for script_name in ("dev", "build", "preview"):
                if script_name in scripts:
                    ok(f"frontend npm script exists: {script_name}")
                else:
                    fail(f"frontend npm script missing: {script_name}")
                    passed = False
        except Exception as exc:
            fail(f"could not read frontend package.json: {exc}")
            passed = False

    return passed


def check_backend_import_and_health() -> bool:
    passed = True
    passed &= check_path(BACKEND / "app" / "main.py", "backend app entrypoint exists")
    passed &= check_path(BACKEND / "requirements.txt", "backend requirements.txt exists")
    passed &= check_path(ROOT / ".env", "backend local .env exists")

    requirements_path = BACKEND / "requirements.txt"
    if requirements_path.exists():
        requirements = requirements_path.read_text(encoding="utf-8").lower()
        for package_name in ("fastapi", "uvicorn"):
            if package_name in requirements:
                ok(f"backend requirement includes {package_name}")
            else:
                fail(f"backend requirement missing {package_name}")
                passed = False

    backend_env_path = ROOT / ".env"
    if backend_env_path.exists():
        backend_env = backend_env_path.read_text(encoding="utf-8")
        for key in ("DATABASE_URL=", "SECRET_KEY=", "LLM_PROVIDER=disabled"):
            if key in backend_env:
                ok(f"backend .env includes {key.rstrip('=')}")
            else:
                fail(f"backend .env missing {key}")
                passed = False

    if str(BACKEND) not in sys.path:
        sys.path.insert(0, str(BACKEND))

    try:
        module = importlib.import_module("app.main")
        app = getattr(module, "app")
        ok("backend imports app.main:app")
    except ModuleNotFoundError as exc:
        fail(f"backend import dependency missing: {exc.name}")
        print("       Run: python -m pip install -r backend/requirements.txt")
        return False
    except Exception as exc:
        fail(f"backend import failed: {exc}")
        return False

    route_paths = {getattr(route, "path", "") for route in getattr(app, "routes", [])}
    if "/healthz" in route_paths:
        ok("backend /healthz route is registered")
    else:
        fail("backend /healthz route is not registered")
        passed = False

    try:
        from fastapi.testclient import TestClient

        response = TestClient(app).get("/healthz")
        if response.status_code == 200:
            ok("backend /healthz returns 200")
        else:
            fail(f"backend /healthz returned {response.status_code}")
            passed = False
    except Exception as exc:
        fail(f"backend /healthz request failed: {exc}")
        passed = False

    return passed


def check_render_config() -> bool:
    render_path = ROOT / "render.yaml"
    if not check_path(render_path, "Render config exists"):
        return False

    passed = True
    render_config = render_path.read_text(encoding="utf-8")
    expected_fragments = {
        "rootDir: backend": "Render rootDir points at backend",
        "buildCommand: pip install -r requirements.txt": "Render installs backend requirements",
        "startCommand: uvicorn app.main:app": "Render starts uvicorn app.main:app",
        "healthCheckPath: /healthz": "Render health check uses /healthz",
    }
    for fragment, label in expected_fragments.items():
        if fragment in render_config:
            ok(label)
        else:
            fail(f"{label} missing")
            passed = False

    return passed


def main() -> int:
    print("AI Resume Analyzer smoke test")
    print("=" * 31)

    checks = [
        check_backend_import_and_health(),
        check_frontend_config(),
        check_render_config(),
        check_path(ROOT / "README.md", "single root README exists"),
    ]

    if all(checks):
        print("\nSmoke test passed.")
        return 0

    print("\nSmoke test failed.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
