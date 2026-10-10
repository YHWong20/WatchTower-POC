# Phase 7: demo, record and replay

Use the demo-scenario skill.
- `make demo` runs the full storyline; `make demo MODE=investigate` the short one.
- `make record NAME=x` captures every `wt.*` stream event with timestamps plus the stand-ins'
  alerts; `make replay NAME=x [SPEED=10]` replays them into a fresh Redis so the Copilot
  shows the same screens with the lab and LLM stopped.
Done when: one full rehearsal meets the definition of done in MASTER_PROMPT.md and its replay
reproduces both screens.
