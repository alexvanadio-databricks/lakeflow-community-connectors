# Bugfix: Remove Always-Null Fields from Ideas Schema

## Problem

Three fields in the `ideas` table schema are always null: `votes`, `endorsements_count`, `comments_count`.

## Root Cause

The Aha! API list endpoint (`GET /ideas`) does not return these fields. They are only available from the single idea endpoint (`GET /ideas/{id}`), which the connector does not use for performance reasons.

## Fix

1. Remove these fields from the `ideas` schema in `aha.py` (lines 75-77):
   - `StructField("votes", LongType())`
   - `StructField("endorsements_count", LongType())`
   - `StructField("comments_count", LongType())`

2. Update tests to remove assertions for these fields

3. Run tests: `pytest sources/aha/test/ -v`
