# Development

## Environment

Everything needed (Rust `1.100.0` from `rust-toolchain.toml`, Python, `uv`, Redis, Postgres 17, Go) is provided by the Nix dev shell:

```sh
nix develop
```

Use `uv` 0.12.4 (`uv sync --locked`) for development and builds; the Nix shell pins that exact version automatically.

## Build and test

```sh
make dev            # build the Rust extension into .venv (editable)
make check          # mypy, stubtest, clippy, rustfmt, ruff, ty
make test           # cargo test + pytest (spawns Redis and Postgres; Go interop needs go)
make test-ft        # pytest on free-threaded Python 3.14t in .venv-ft; fails if GIL turns on
make build          # uv build --no-sources
make release-check  # verify wheel and sdist outside the checkout (pass TAG=vX.Y.Z to check tag)
make redis          # run a local non-persistent Redis on port 6379
```

Editable sync compiles the extension into the source tree; rerun `uv sync --reinstall-package spater` after Rust changes.

## Repository layout

| Path | Contents |
| --- | --- |
| `src/` | Rust engine and PyO3 bindings (`_core`) |
| `src/rdb/lua/` | Redis Lua scripts |
| `src/rdb/pg/` | Postgres backend and SQL schema |
| `src/rdb/memory.rs` | In-process memory broker |
| `python/spater/` | Python API, CLI, type stubs (`_core.pyi`), and `spater.flow` |
| `interop/go/` | Small Go program used by tests to verify compatibility with Go asynq |
| `examples/flows/` | Example DAG definitions and generated graphs |
| `ops/` | Local workflow operations center (Rust Axum API + TanStack Start UI) |

## Workflow operations center

Start the local console from the repository root with Node 22.12+, Rust, and `uv`. `--demo` runs the example jobs in a named in-memory store:

```sh
uv sync --locked
npm ci --prefix ops/web
node ops/dev.mjs --demo
```

- Open `http://127.0.0.1:3410/runs` to inspect root runs, attempt timing, saved graphs, output previews, and mapped children.
- Open **Workflows** to inspect registered definitions, launch runs with JSON input, or trigger linked reruns that diff input, definition, failure, and step outcomes.
- To load your own jobs, set `SPATER_OPS_JOBS` to an importable `module:attribute` sequence of `Job` definitions and run `node ops/dev.mjs`.
- Set `SPATER_OPS_STORE` to the same PostgreSQL URL as your runner to inspect runs from other processes (defaults to `memory://spater-ops`). Console launches enqueue to the `spater-ops` queue.

The console is a trusted local tool listening on loopback ports `3410` (web) and `8410` (Rust API). Forward port `3410` over SSH for remote access.

### OpenAPI contract and production build

Rust handler DTOs define the API contract; Axum serves the utoipa spec at `http://127.0.0.1:8410/openapi.json`.

```sh
# Regenerate or verify checked-in OpenAPI spec and TypeScript types
npm --prefix ops/web run gen:openapi
npm --prefix ops/web run check:openapi

# Production build
npm --prefix ops/web run build
cargo run --locked --manifest-path ops/api/Cargo.toml -- serve
npm --prefix ops/web start

# End-to-end browser tests (starts its own API, worker, and web listener)
npx --prefix ops/web playwright install chromium
npm --prefix ops/web run test:e2e
```

## Installing from GitHub Releases

Each GitHub release includes `SHA256SUMS` and an `index.html` flat package index with hashed wheel links:

```sh
uv pip install --no-index --find-links 'https://github.com/denismaciel/spater/releases/download/v0.6.1/index.html' 'spater==0.6.1'
```

Note: the distribution, Python import, and CLI are `spater` (legacy aliases were removed). Existing PostgreSQL storage identifiers (`pyasynq_tasks`) remain unchanged so upgrades preserve queued tasks.

## Packaging and releases

The in-tree `build_backend.py` delegates Python metadata, sdists, and wheel layout to `uv_build==0.12.4`, then compiles the Rust extension with locked Cargo dependencies, inserts it into the wheel, and updates platform/ABI tags and `RECORD` hashes. No Maturin or Hatch backend is used.

Keep `project.version` in `pyproject.toml` and `version` in `Cargo.toml` identical, then validate locally:

```sh
make release-check TAG=vX.Y.Z
```

Release workflow guarantees:

- Only `vX.Y.Z` tag pushes reachable from the default branch in the canonical repository publish; manual workflow dispatch builds only.
- Standard CPython wheels use `abi3` (Python 3.10+); free-threaded 3.14t and 3.15t build separate ABI wheels. `cibuildwheel` repairs Linux wheels in `manylinux`/`musllinux` containers and smoke-tests every wheel natively.
- GitHub release assets (`SHA256SUMS`, flat `index.html`, wheels, sdist) are published first. A separate `pypi` environment job then publishes those exact artifacts via [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/) with [PEP 740 attestations](https://docs.pypi.org/attestations/producing-attestations/) (owner `denismaciel`, repo `spater`, workflow `release.yml`, environment `pypi`, no checkout or build step, no long-lived token).
- PyPI files cannot be replaced. If PyPI upload fails after the GitHub release succeeds, resolve the configuration issue and rerun only the failed `pypi` job; do not rerun GitHub publication.
