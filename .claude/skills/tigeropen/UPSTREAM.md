# Tiger OpenAPI AI Skill (Python)

Vendored from https://github.com/tigerfintech/tigeropen-skill, `skills/tigeropen/`
on 2026-06-17 for use by Claude Code sessions working on this repo's Tiger
integration. Provides authoritative SDK references so we stop guessing at
combo_contract shapes, error codes, and per-endpoint conventions.

To refresh:
```bash
git clone --depth 1 https://github.com/tigerfintech/tigeropen-skill.git /tmp/tigeropen-skill
rm -rf .claude/skills/tigeropen
cp -r /tmp/tigeropen-skill/skills/tigeropen .claude/skills/tigeropen
```

Disclaimer + license terms live in the upstream repo's LICENSE / DISCLAIMER files.
