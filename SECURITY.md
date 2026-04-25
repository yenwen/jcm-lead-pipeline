# Security Checklist

## Secrets

- Keep real credentials only in local `.env`
- Never commit API keys, tokens, or database dumps
- Rotate any secret immediately if it is exposed

## Files that should stay out of Git

- `.env`
- `.env.*`
- `*.db`
- exported lead lists
- raw vendor exports
- temporary research dumps
- browser/session artifacts

## Before pushing

- Review `git status`
- Review staged files with `git diff --cached`
- Confirm no credentials or local data files are included

## If a secret is exposed

1. Rotate the secret immediately
2. Remove it from tracked files
3. Rewrite history if needed
4. Re-check the remote repo contents
