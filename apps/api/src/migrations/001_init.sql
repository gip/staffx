-- StaffX full schema init (dev-only, destructive)
-- Order: users → openship core → projects → threads

create extension if not exists pgcrypto;

-- ============================================================
-- USERS
-- ============================================================

create table if not exists users (
  id              uuid primary key default gen_random_uuid(),
  auth0_id        text unique not null,
  email           text,
  name            text,
  picture         text,
  handle          text unique not null,
  github_handle   text,
  created_at      timestamptz default now(),
  updated_at      timestamptz default now()
);

-- ============================================================
-- OPENSHIP ENUMS
-- ============================================================

create type node_kind as enum ('System', 'Host', 'Container', 'Process', 'Library');
create type edge_type as enum ('Runtime', 'Dataflow', 'Dependency');
create type doc_kind as enum ('Feature', 'Spec', 'Skill');
create type ref_type as enum ('Feature', 'Spec', 'Skill');
create type artifact_type as enum ('Summary', 'Code', 'Docs');

-- ============================================================
-- SYSTEMS
-- ============================================================

create table systems (
  id              text primary key,
  name            text not null,
  spec_version    text not null default 'openship/v1',
  system_node_id  text not null,
  metadata        jsonb not null default '{}',
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

-- ============================================================
-- CONCERNS
-- ============================================================

create table concerns (
  system_id   text not null references systems(id) on delete cascade,
  name        text not null,
  position    int not null,
  is_baseline bool not null default false,
  scope       text,
  primary key (system_id, name)
);

create index idx_concerns_order on concerns (system_id, position);

-- ============================================================
-- NODES
-- ============================================================

create table nodes (
  id         text not null,
  system_id  text not null references systems(id) on delete cascade,
  kind       node_kind not null,
  name       text not null,
  parent_id  text,
  metadata   jsonb not null default '{}',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (system_id, id),
  constraint fk_node_parent foreign key (system_id, parent_id)
    references nodes(system_id, id) deferrable initially deferred
);

create index idx_nodes_parent on nodes (system_id, parent_id);
create index idx_nodes_kind on nodes (system_id, kind);

-- ============================================================
-- EDGES
-- ============================================================

create table edges (
  id           text not null,
  system_id    text not null references systems(id) on delete cascade,
  type         edge_type not null,
  from_node_id text not null,
  to_node_id   text not null,
  metadata     jsonb not null default '{}',
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  primary key (system_id, id),
  constraint fk_edge_from foreign key (system_id, from_node_id)
    references nodes(system_id, id) on delete cascade,
  constraint fk_edge_to foreign key (system_id, to_node_id)
    references nodes(system_id, id) on delete cascade
);

create index idx_edges_from on edges (system_id, from_node_id);
create index idx_edges_to on edges (system_id, to_node_id);
create index idx_edges_type on edges (system_id, type);

-- ============================================================
-- DOCUMENTS (content-addressed by hash)
-- ============================================================

create table documents (
  hash        text not null,
  system_id   text not null references systems(id) on delete cascade,
  kind        doc_kind not null,
  title       text not null,
  language    text not null default 'en',
  text        text not null,
  supersedes  text,
  created_at  timestamptz not null default now(),
  primary key (system_id, hash)
);

create index idx_documents_kind on documents (system_id, kind);
create index idx_documents_supersedes on documents (system_id, supersedes)
  where supersedes is not null;

-- ============================================================
-- MATRIX REFS (concern × node → documents)
-- ============================================================

create table matrix_refs (
  system_id  text not null,
  node_id    text not null,
  concern    text not null,
  ref_type   ref_type not null,
  doc_hash   text not null,
  primary key (system_id, node_id, concern, ref_type, doc_hash),
  foreign key (system_id, node_id) references nodes(system_id, id) on delete cascade,
  foreign key (system_id, concern) references concerns(system_id, name) on delete cascade,
  foreign key (system_id, doc_hash) references documents(system_id, hash) on delete cascade
);

create index idx_matrix_cell on matrix_refs (system_id, node_id, concern);
create index idx_matrix_doc on matrix_refs (system_id, doc_hash);
create index idx_matrix_concern on matrix_refs (system_id, concern);

-- ============================================================
-- ARTIFACTS
-- ============================================================

create table artifacts (
  id         text not null,
  system_id  text not null,
  node_id    text not null,
  concern    text not null,
  type       artifact_type not null,
  language   text not null default 'en',
  text       text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (system_id, id),
  foreign key (system_id, node_id) references nodes(system_id, id) on delete cascade,
  foreign key (system_id, concern) references concerns(system_id, name) on delete cascade
);

create index idx_artifacts_node on artifacts (system_id, node_id);
create index idx_artifacts_cell on artifacts (system_id, node_id, concern);

-- ============================================================
-- FILE CONTENTS (content-addressed, global)
-- ============================================================

create table file_contents (
  hash         text primary key,
  file_path    text not null,
  file_content text not null,
  created_at   timestamptz not null default now()
);

-- ============================================================
-- ARTIFACT FILES
-- ============================================================

create table artifact_files (
  system_id    text not null,
  artifact_id  text not null,
  file_hash    text not null references file_contents(hash),
  primary key (system_id, artifact_id, file_hash),
  foreign key (system_id, artifact_id) references artifacts(system_id, id) on delete cascade
);

create index idx_artifact_files_artifact on artifact_files (system_id, artifact_id);
create index idx_artifact_files_hash on artifact_files (file_hash);

-- ============================================================
-- PROJECTS
-- ============================================================

create type collaborator_role as enum ('Editor', 'Viewer');

create table projects (
  id          text primary key,
  name        text not null,
  description text,
  owner_id    uuid not null references users(id),
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

create index idx_projects_owner on projects (owner_id);

create table project_collaborators (
  project_id  text not null references projects(id) on delete cascade,
  user_id     uuid not null references users(id) on delete cascade,
  role        collaborator_role not null default 'Editor',
  created_at  timestamptz not null default now(),
  primary key (project_id, user_id)
);

create index idx_collaborators_user on project_collaborators (user_id);

-- ============================================================
-- THREADS
-- ============================================================

create type action_type as enum ('Chat', 'Edit', 'Import', 'Custom');
create type message_role as enum ('User', 'Assistant', 'System');
create type change_operation as enum ('Create', 'Update', 'Delete');

create table threads (
  id               text primary key,
  title            text,
  project_id       text not null references projects(id) on delete cascade,
  created_by       uuid not null references users(id),
  seed_system_id   text not null references systems(id),
  source_thread_id text references threads(id),
  status           text not null default 'open'
    check (status in ('open', 'closed')),
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);

create index idx_threads_project on threads (project_id);
create index idx_threads_created_by on threads (created_by);
create index idx_threads_seed on threads (seed_system_id);
create index idx_threads_source on threads (source_thread_id) where source_thread_id is not null;
create index idx_threads_status on threads (status);

-- ============================================================
-- ACTIONS
-- ============================================================

create table actions (
  id               text not null,
  thread_id        text not null references threads(id) on delete cascade,
  position         int not null,
  type             action_type not null,
  title            text,
  output_system_id text references systems(id),
  created_at       timestamptz not null default now(),
  primary key (thread_id, id),
  unique (thread_id, position)
);

create index idx_actions_order on actions (thread_id, position);
create index idx_actions_output on actions (output_system_id) where output_system_id is not null;

-- ============================================================
-- MESSAGES
-- ============================================================

create table messages (
  id          text not null,
  thread_id   text not null,
  action_id   text not null,
  role        message_role not null,
  content     text not null,
  position    int not null,
  created_at  timestamptz not null default now(),
  primary key (thread_id, action_id, id),
  foreign key (thread_id, action_id) references actions(thread_id, id) on delete cascade
);

create index idx_messages_order on messages (thread_id, action_id, position);

-- ============================================================
-- CHANGES
-- ============================================================

create table changes (
  id            text not null,
  thread_id     text not null,
  action_id     text not null,
  target_table  text not null,
  operation     change_operation not null,
  target_id     jsonb not null,
  previous      jsonb,
  current       jsonb,
  created_at    timestamptz not null default now(),
  primary key (thread_id, action_id, id),
  foreign key (thread_id, action_id) references actions(thread_id, id) on delete cascade
);

create index idx_changes_action on changes (thread_id, action_id);
create index idx_changes_target on changes (target_table, target_id);

-- ============================================================
-- FUNCTIONS
-- ============================================================

create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create or replace function upsert_file_content(
  p_file_path text,
  p_file_content text
) returns text as $$
declare
  v_hash text;
begin
  v_hash := 'sha256:' || encode(
    sha256(convert_to(p_file_path || E'\n' || p_file_content, 'UTF8')),
    'hex'
  );

  insert into file_contents (hash, file_path, file_content)
  values (v_hash, p_file_path, p_file_content)
  on conflict (hash) do nothing;

  return v_hash;
end;
$$ language plpgsql;

create or replace function fork_system(
  source_system_id text,
  new_system_id text,
  new_system_name text default null
) returns text as $$
begin
  insert into systems (id, name, spec_version, system_node_id, metadata)
  select new_system_id,
         coalesce(new_system_name, name),
         spec_version,
         system_node_id,
         metadata
  from systems where id = source_system_id;

  insert into concerns (system_id, name, position, is_baseline, scope)
  select new_system_id, name, position, is_baseline, scope
  from concerns where system_id = source_system_id;

  insert into nodes (id, system_id, kind, name, parent_id, metadata)
  select id, new_system_id, kind, name, parent_id, metadata
  from nodes where system_id = source_system_id;

  insert into edges (id, system_id, type, from_node_id, to_node_id, metadata)
  select id, new_system_id, type, from_node_id, to_node_id, metadata
  from edges where system_id = source_system_id;

  insert into documents (hash, system_id, kind, title, language, text, supersedes)
  select hash, new_system_id, kind, title, language, text, supersedes
  from documents where system_id = source_system_id;

  insert into matrix_refs (system_id, node_id, concern, ref_type, doc_hash)
  select new_system_id, node_id, concern, ref_type, doc_hash
  from matrix_refs where system_id = source_system_id;

  insert into artifacts (id, system_id, node_id, concern, type, language, text)
  select id, new_system_id, node_id, concern, type, language, text
  from artifacts where system_id = source_system_id;

  insert into artifact_files (system_id, artifact_id, file_hash)
  select new_system_id, artifact_id, file_hash
  from artifact_files where system_id = source_system_id;

  return new_system_id;
end;
$$ language plpgsql;

create or replace function thread_current_system(p_thread_id text)
returns text as $$
  select coalesce(
    (
      select a.output_system_id
      from actions a
      where a.thread_id = p_thread_id
        and a.output_system_id is not null
      order by a.position desc
      limit 1
    ),
    (select t.seed_system_id from threads t where t.id = p_thread_id)
  );
$$ language sql stable;

create or replace function create_thread(
  p_thread_id text,
  p_project_id text,
  p_created_by uuid,
  p_seed_system_id text,
  p_title text default null
) returns text as $$
begin
  insert into threads (id, title, project_id, created_by, seed_system_id, status)
  values (p_thread_id, p_title, p_project_id, p_created_by, p_seed_system_id, 'open');

  return p_thread_id;
end;
$$ language plpgsql;

create or replace function clone_thread(
  p_new_thread_id text,
  p_source_thread_id text,
  p_project_id text,
  p_created_by uuid,
  p_title text default null
) returns text as $$
declare
  v_current_system text;
begin
  v_current_system := thread_current_system(p_source_thread_id);

  insert into threads (id, title, project_id, created_by, seed_system_id, source_thread_id, status)
  values (p_new_thread_id, p_title, p_project_id, p_created_by, v_current_system, p_source_thread_id, 'open');

  return p_new_thread_id;
end;
$$ language plpgsql;

create or replace function begin_action(
  p_thread_id text,
  p_action_id text,
  p_type action_type,
  p_title text default null
) returns text as $$
declare
  v_current_system text;
  v_new_system text;
  v_position int;
begin
  if not exists (select 1 from threads where id = p_thread_id and status = 'open') then
    raise exception 'Thread % is not open', p_thread_id;
  end if;

  v_current_system := thread_current_system(p_thread_id);
  v_position := coalesce(
    (select max(position) + 1 from actions where thread_id = p_thread_id),
    1
  );

  v_new_system := v_current_system || '.a.' || p_action_id;
  perform fork_system(v_current_system, v_new_system);

  insert into actions (id, thread_id, position, type, title, output_system_id)
  values (p_action_id, p_thread_id, v_position, p_type, p_title, v_new_system);

  return v_new_system;
end;
$$ language plpgsql;

create or replace function commit_action_empty(
  p_thread_id text,
  p_action_id text
) returns void as $$
declare
  v_system_to_drop text;
begin
  select output_system_id into v_system_to_drop
  from actions where thread_id = p_thread_id and id = p_action_id;

  if v_system_to_drop is not null then
    update actions set output_system_id = null
    where thread_id = p_thread_id and id = p_action_id;

    delete from systems where id = v_system_to_drop;
  end if;
end;
$$ language plpgsql;

create or replace function close_thread(p_thread_id text) returns void as $$
begin
  update threads set status = 'closed', updated_at = now()
  where id = p_thread_id;
end;
$$ language plpgsql;

create or replace function diff_artifact_files(
  system_a text,
  system_b text,
  p_artifact_id text
) returns table (
  file_path text,
  status text,
  old_hash text,
  new_hash text
) as $$
  select
    coalesce(fa.file_path, fb.file_path) as file_path,
    case
      when fa.hash is null then 'added'
      when fb.hash is null then 'removed'
      when fa.hash != fb.hash then 'modified'
      else 'unchanged'
    end as status,
    fa.hash as old_hash,
    fb.hash as new_hash
  from (
    select fc.hash, fc.file_path
    from artifact_files af
    join file_contents fc on fc.hash = af.file_hash
    where af.system_id = system_a and af.artifact_id = p_artifact_id
  ) fa
  full outer join (
    select fc.hash, fc.file_path
    from artifact_files af
    join file_contents fc on fc.hash = af.file_hash
    where af.system_id = system_b and af.artifact_id = p_artifact_id
  ) fb on fa.file_path = fb.file_path;
$$ language sql;

create or replace function diff_thread(
  p_thread_id text
) returns table (
  target_table text,
  operation text,
  target_id jsonb,
  action_id text,
  action_position int
) as $$
  select
    c.target_table,
    c.operation::text,
    c.target_id,
    c.action_id,
    a.position as action_position
  from changes c
  join actions a on a.thread_id = c.thread_id and a.id = c.action_id
  where c.thread_id = p_thread_id
  order by a.position, c.id;
$$ language sql;

-- ============================================================
-- TRIGGERS
-- ============================================================

create trigger trg_systems_updated   before update on systems   for each row execute function set_updated_at();
create trigger trg_nodes_updated     before update on nodes     for each row execute function set_updated_at();
create trigger trg_edges_updated     before update on edges     for each row execute function set_updated_at();
create trigger trg_artifacts_updated before update on artifacts for each row execute function set_updated_at();
create trigger trg_projects_updated  before update on projects  for each row execute function set_updated_at();
create trigger trg_threads_updated   before update on threads   for each row execute function set_updated_at();

-- ============================================================
-- VIEWS
-- ============================================================

create materialized view matrix_view as
select
  mr.system_id,
  mr.node_id,
  mr.concern,
  jsonb_agg(mr.doc_hash) filter (where mr.ref_type = 'Feature') as feature_refs,
  jsonb_agg(mr.doc_hash) filter (where mr.ref_type = 'Spec')    as spec_refs,
  jsonb_agg(mr.doc_hash) filter (where mr.ref_type = 'Skill')   as skill_refs
from matrix_refs mr
group by mr.system_id, mr.node_id, mr.concern;

create unique index idx_matrix_view_pk on matrix_view (system_id, node_id, concern);

create view artifact_files_view as
select
  af.system_id,
  af.artifact_id,
  fc.hash as file_hash,
  fc.file_path,
  fc.file_content
from artifact_files af
join file_contents fc on fc.hash = af.file_hash;

create view node_overview as
select
  n.system_id,
  n.id,
  n.kind,
  n.name,
  n.parent_id,
  (select count(*) from nodes c where c.system_id = n.system_id and c.parent_id = n.id) as child_count,
  (select count(*) from edges e where e.system_id = n.system_id and e.from_node_id = n.id) as outbound_edges,
  (select count(*) from edges e where e.system_id = n.system_id and e.to_node_id = n.id) as inbound_edges,
  (select count(*) from matrix_refs mr where mr.system_id = n.system_id and mr.node_id = n.id) as matrix_ref_count,
  (select count(*) from artifacts a where a.system_id = n.system_id and a.node_id = n.id) as artifact_count
from nodes n;

create view user_projects as
select
  p.id,
  p.name,
  p.description,
  p.owner_id,
  p.created_at,
  u.id as user_id,
  case
    when p.owner_id = u.id then 'Owner'
    else pc.role::text
  end as access_role
from projects p
cross join users u
left join project_collaborators pc on pc.project_id = p.id and pc.user_id = u.id
where p.owner_id = u.id or pc.user_id is not null;

create view thread_timeline as
select
  a.thread_id,
  a.id as action_id,
  a.position,
  a.type,
  a.title,
  coalesce(
    (select prev.output_system_id
     from actions prev
     where prev.thread_id = a.thread_id
       and prev.position < a.position
       and prev.output_system_id is not null
     order by prev.position desc
     limit 1),
    t.seed_system_id
  ) as input_system_id,
  a.output_system_id,
  (select count(*) from changes c
   where c.thread_id = a.thread_id and c.action_id = a.id) as change_count,
  a.created_at
from actions a
join threads t on t.id = a.thread_id
order by a.thread_id, a.position;

create view project_summary as
select
  p.id,
  p.name,
  p.description,
  p.owner_id,
  (select count(*) from threads t where t.project_id = p.id) as thread_count,
  (select count(*) from threads t where t.project_id = p.id and t.status = 'open') as open_threads,
  (select count(*) from project_collaborators pc where pc.project_id = p.id) as collaborator_count,
  p.created_at,
  p.updated_at
from projects p;
