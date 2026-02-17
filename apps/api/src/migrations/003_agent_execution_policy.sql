-- Add project execution policy and agent run metadata persisted for execution routing and audit.

alter table projects
  add column if not exists agent_execution_mode text;

update projects
  set agent_execution_mode = 'both'
  where agent_execution_mode is null;

alter table projects
  alter column agent_execution_mode set default 'both';

alter table projects
  alter column agent_execution_mode set not null;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'projects'::regclass
      AND conname = 'projects_agent_execution_mode_check'
  ) THEN
    ALTER TABLE projects
      ADD CONSTRAINT projects_agent_execution_mode_check
      CHECK (agent_execution_mode IN ('desktop', 'backend', 'both'));
  END IF;
END
$$;

alter table agent_runs
  add column if not exists executor text;

alter table agent_runs
  add column if not exists model text;

update agent_runs
  set executor = 'backend'
  where executor is null;

update agent_runs
  set model = 'claude-opus-4-6'
  where model is null;

alter table agent_runs
  alter column executor set default 'backend';

alter table agent_runs
  alter column model set default 'claude-opus-4-6';

alter table agent_runs
  alter column executor set not null;

alter table agent_runs
  alter column model set not null;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'agent_runs'::regclass
      AND conname = 'agent_runs_executor_check'
  ) THEN
    ALTER TABLE agent_runs
      ADD CONSTRAINT agent_runs_executor_check
      CHECK (executor IN ('backend', 'desktop'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'agent_runs'::regclass
      AND conname = 'agent_runs_model_check'
  ) THEN
    ALTER TABLE agent_runs
      ADD CONSTRAINT agent_runs_model_check
      CHECK (model IN ('claude-opus-4-6', 'gpt-5.3-codex'));
  END IF;
END
$$;
