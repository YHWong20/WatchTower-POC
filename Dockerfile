# One small image for the simulators now, and the agents later (different commands).
FROM python:3.12-slim AS build
RUN pip install --no-cache-dir uv==0.13.0
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --no-cache

FROM python:3.12-slim
WORKDIR /app
ENV PATH=/app/.venv/bin:$PATH PYTHONPATH=/app PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY --from=build /app/.venv /app/.venv
# A fixed SSH host key per image build, so `ssh -p 2221` does not warn on every restart.
RUN python -c "import asyncssh, pathlib; p = pathlib.Path('/etc/wt-sim'); p.mkdir(); \
asyncssh.generate_private_key('ssh-ed25519').write_private_key(p / 'ssh_host_ed25519_key')" \
 && chown -R 10001 /etc/wt-sim
COPY contracts ./contracts
COPY playbooks ./playbooks
COPY wt ./wt
COPY sims ./sims
USER 10001
