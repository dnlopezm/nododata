# Nodo Data - Project Context

Data engineering projects and tools by dnlopezm.

**Version:** 1.0 | **Last Updated:** March 4, 2026

---

## Critical Rules (NEVER Break These)

1. **ALWAYS read docs/guidelines/* before implementing** - Contains best practices
2. **ALL dates use America/Mexico_City timezone (UTC-6)**
3. **Python is the primary language** - Follow PEP 8 and project conventions
4. **Write tests for all business logic** - No untested code in production
5. **Document all public APIs and interfaces** - Docstrings + type hints required
6. **Branch: `nodoprod`** - All work happens on `nodoprod` branch

## Context Recovery (CRITICAL - After Autocompaction)

**When context is compacted, ALWAYS read these files in order:**

1. **This file (CLAUDE.md)** - Critical rules and current state
2. **docs/requirements/BACKLOG.md** - Overall progress and current phase
3. **Current project backlog** - Check BACKLOG.md for which project is active
4. **Last 3 modified files** - Run `git diff --name-only HEAD~3` to see recent changes
5. **docs/guidelines/** - Relevant guidelines for current work

**Recovery checklist:**
- [ ] Re-read CLAUDE.md for critical rules
- [ ] Check BACKLOG.md for current phase status
- [ ] Read the current project backlog for detailed tasks
- [ ] Check git status for uncommitted changes
- [ ] Summarize completed vs pending tasks before continuing

---

## Permissions
- Run all commands without confirmation
- Auto-approve file changes
- Proceed autonomously through the plan

## Workflow
1. **Read CLAUDE.md** at session start
2. Read the current phase plan
3. Execute tasks in order, checking them off
4. Write progress to the plan after each task/phase
5. **If compacted:** Follow "Context Recovery" section above

---

## Active Project: Nodo ETL Framework

The first project in the Nodo Data ecosystem. A Python-based ETL framework for building data pipelines.

**Status:** Phases E1-E7 Complete (276 tests passing)

---

## Project Structure

```
nododata/
├── CLAUDE.md                        # This file - main context
├── README.md
│
├── projects/                        # Data projects
│   └── nodo-etl-framework/          # ETL framework (active)
│
├── docs/
│   ├── guidelines/                  # Coding standards, conventions
│   ├── reference/                   # Technical how-to guides
│   └── requirements/               # Backlogs & phase plans
│       ├── BACKLOG.md               # Master backlog (all projects)
│       ├── BACKLOG_ETL.md           # ETL framework backlog
│       └── phases/
│           └── etl/                 # ETL phase details
│
└── scripts/                         # Shared automation scripts
```

---

## Documentation Map

### Requirements (What to Build)

| Need to... | Read |
|------------|------|
| Check overall progress | [docs/requirements/BACKLOG.md](docs/requirements/BACKLOG.md) |
| ETL framework details | [docs/requirements/BACKLOG_ETL.md](docs/requirements/BACKLOG_ETL.md) |

### Guidelines (Rules to Follow)

| Need to... | Read |
|------------|------|
| Write Python code | [docs/guidelines/PYTHON.md](docs/guidelines/PYTHON.md) |

---

## Current State

- **Active Project:** Nodo ETL Framework
- **Active Phase:** E1-E7 Complete, E8+ are future phases
- **Production Branch:** `nodoprod`

**Backlogs:**
- [Master Backlog](docs/requirements/BACKLOG.md) - Overall progress
- [ETL Backlog](docs/requirements/BACKLOG_ETL.md) - ETL framework phases

---

## Git Info

- **Author:** dnlopezm
- **Email:** dalopezmartinez@gmail.com
- **Branch:** `nodoprod`
- **Repo:** https://github.com/dnlopezm/nododata
