-- PostgREST role provisioning + auto schema-cache reload trigger.
--
-- Run by the tikoguest `PostgRest` service (via psql) before spawning
-- postgrest. Idempotent: safe to run on every start.
--
-- Trust-the-TAP-subnet model (JWT deferred):
--   authenticator  - LOGIN role PostgREST connects as (NOINHERIT, so it must
--                    explicitly SET ROLE to gain any privileges).
--   anon           - least-privilege role for unauthenticated requests.
-- PostgREST connects as `authenticator` (trust auth on the local subnet) and
-- impersonates `anon`.

-- Connection role.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticator') THEN
    CREATE ROLE authenticator NOINHERIT LOGIN;
  END IF;
END $$;

-- Anonymous (unauthenticated) role.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    CREATE ROLE anon NOLOGIN;
  END IF;
END $$;

-- authenticator may impersonate anon.
GRANT anon TO authenticator;

-- anon gets read access to the public schema (existing + future tables).
GRANT usage ON SCHEMA public TO anon;
GRANT select ON ALL TABLES IN SCHEMA public TO anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT select ON TABLES TO anon;

-- Auto schema-cache reload: PostgREST LISTENs on the `pgrst` channel and
-- rebuilds its cache on NOTIFY. This event trigger fires the NOTIFY whenever
-- DDL lands, so schema changes are picked up without a manual reload
-- (POST /services/postgrest/reload sends SIGUSR2 as a manual fallback).
CREATE OR REPLACE FUNCTION public.pgrst_watch() RETURNS event_trigger
  LANGUAGE plpgsql AS $$ BEGIN NOTIFY pgrst; END $$;

DROP EVENT TRIGGER IF EXISTS pgrst_watch_ddl;
CREATE EVENT TRIGGER pgrst_watch_ddl ON ddl_command_end
  EXECUTE FUNCTION public.pgrst_watch();
