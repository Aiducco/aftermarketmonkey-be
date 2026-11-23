#!/usr/bin/env python3
"""PreToolUse (Bash): block the git and manage.py commands the repo rules forbid, precisely.

Replaces glob deny rules such as `Bash(git * push*)`, which matched the word "push" anywhere (commit messages,
heredocs containing `on: push:`), and `Bash(git checkout*)`, which also blocked throwaway repos in a scratchpad.
This parses the command instead: it finds each real `git` invocation, its subcommand and the repository it acts on.

Blocked:
  - `git push`, anywhere (a push to main or staging deploys).
  - `git stash|reset|clean|checkout|switch|restore|merge|rebase|pull` in a protected repo: the workspace, be, fe and
    their worktrees (same git common dir). Other repos (scratch, /tmp) are allowed.
  - `gh pr merge`.
  - `manage.py migrate|flush|shell|dbshell` unless the command names --settings=conf.settings_{test,dev,staging}
    (default settings are the PRODUCTION database; settings_staging refuses the production host itself).
Exit 2 blocks the call and shows the reason to the agent. Any parse failure blocks too (fail closed).
"""
import json
import os
import re
import shlex
import subprocess
import sys

PROTECTED_REPOS = [
    "/Users/gojkohajdukovic/code/aftermarketscout/workspace",
    "/Users/gojkohajdukovic/Documents/code/aftermarketmonkey-be",
    "/Users/gojkohajdukovic/Documents/code/aftermarketmonkey-fe",
]
PROTECTED_SUBCOMMANDS = {"stash", "reset", "clean", "checkout", "switch", "restore", "merge", "rebase", "pull"}
GIT_OPTS_WITH_ARG = {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path", "--config-env"}
SAFE_SETTINGS = re.compile(r"--settings[= ]conf\.settings_(test|dev|staging)\b")
DB_SUBCOMMANDS = {"migrate", "flush", "shell", "dbshell"}
SEPARATORS = {";", "&&", "||", "|", "&", "(", ")", "{", "}", "\n", "|&", ";;"}
WRAPPERS = {"command", "env", "sudo", "time", "nohup", "exec", "xargs", "then", "do", "else", "if", "while", "!"}


def common_dir(path):
    try:
        out = subprocess.run(
            ["git", "-C", path, "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True, text=True, timeout=5,
        )
    except Exception:
        return None
    return os.path.realpath(out.stdout.strip()) if out.returncode == 0 else None


def protected_common_dirs():
    dirs = set()
    for repo in PROTECTED_REPOS + [os.environ.get("CLAUDE_PROJECT_DIR", "")]:
        if repo and os.path.isdir(repo):
            d = common_dir(repo)
            if d:
                dirs.add(d)
    return dirs


def strip_heredocs(cmd):
    """Drop heredoc bodies: they are data, not commands."""
    out, lines, i = [], cmd.split("\n"), 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        m = re.search(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1", line)
        i += 1
        if m:
            tag = m.group(2)
            while i < len(lines) and lines[i].strip() != tag:
                i += 1
            i += 1
    return "\n".join(out)


def simple_commands(cmd):
    lex = shlex.shlex(strip_heredocs(cmd).replace("\n", " ; "), posix=True, punctuation_chars=";&|(){}")
    lex.whitespace_split = True
    lex.commenters = ""
    current = []
    for tok in lex:
        if tok in SEPARATORS or set(tok) <= set(";&|(){}"):
            if current:
                yield current
            current = []
        else:
            current.append(tok)
    if current:
        yield current


ASSIGNMENT = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", re.S)


def expand(value, shell_vars):
    """Expand $VAR / ${VAR} from assignments earlier in the command, then the environment."""

    def sub(m):
        name = m.group(1) or m.group(2)
        return shell_vars.get(name, os.environ.get(name, m.group(0)))

    return os.path.expanduser(re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)", sub, value))


def resolve(base, path, shell_vars):
    return os.path.normpath(os.path.join(base, expand(path, shell_vars)))


def check(cmd, cwd):
    protected = None
    shell_vars = {}
    for argv in simple_commands(cmd):
        if argv and argv[0] == "export":
            argv = argv[1:]
        if argv and all(ASSIGNMENT.match(a) for a in argv):  # plain `X=y` lines set shell variables
            for a in argv:
                name, value = ASSIGNMENT.match(a).groups()
                shell_vars[name] = expand(value, shell_vars)
            continue
        while argv and (argv[0] in WRAPPERS or ASSIGNMENT.match(argv[0])):
            argv = argv[1:]
        if not argv:
            continue
        prog = os.path.basename(argv[0])
        if prog == "cd" and len(argv) > 1:
            cwd = resolve(cwd, argv[1], shell_vars)
            continue
        if prog == "gh" and argv[1:3] == ["pr", "merge"]:
            return "gh pr merge is not allowed: agents never merge."
        if any(os.path.basename(a) == "manage.py" for a in argv):
            idx = next(i for i, a in enumerate(argv) if os.path.basename(a) == "manage.py")
            sub = argv[idx + 1] if idx + 1 < len(argv) else ""
            if sub in DB_SUBCOMMANDS and not SAFE_SETTINGS.search(" ".join(argv)):
                return (
                    f"manage.py {sub} without --settings=conf.settings_test|settings_dev|settings_staging would use "
                    "the default settings, which are the PRODUCTION database."
                )
            continue
        if prog != "git":
            continue
        target, i = cwd, 1
        while i < len(argv) and argv[i].startswith("-"):
            opt = argv[i].split("=", 1)[0]
            if opt == "-C" and i + 1 < len(argv):
                target = resolve(target, argv[i + 1], shell_vars)
            i += 2 if (opt in GIT_OPTS_WITH_ARG and "=" not in argv[i]) else 1
        sub = argv[i] if i < len(argv) else ""
        if sub == "push":
            return "git push is not allowed: a push to main or staging deploys. The human pushes."
        if sub in PROTECTED_SUBCOMMANDS:
            if protected is None:
                protected = protected_common_dirs()
            # A repo the same command creates (git init) does not exist yet: judge by its nearest existing parent.
            probe = target
            while not os.path.isdir(probe) and os.path.dirname(probe) != probe:
                probe = os.path.dirname(probe)
            d = common_dir(probe)
            inside_any_repo = subprocess.run(
                ["git", "-C", probe, "rev-parse", "--is-inside-work-tree"], capture_output=True, timeout=5
            ).returncode == 0 or os.path.isdir(os.path.join(probe, ".git"))
            if (d is None and inside_any_repo) or (d is not None and d in protected):
                return (
                    f"git {sub} is not allowed in the workspace, be or fe repos or their worktrees (other sessions "
                    "keep uncommitted work there). Scratch repos are fine."
                )
    return None


def main():
    try:
        data = json.load(sys.stdin)
        if data.get("tool_name") != "Bash":
            return 0
        cmd = data.get("tool_input", {}).get("command", "")
        reason = check(cmd, data.get("cwd") or os.getcwd())
    except Exception as exc:  # fail closed
        reason = f"guard-bash could not parse the command ({exc}); split it into simpler commands."
    if reason:
        print(f"Blocked by .claude/hooks/guard-bash.py: {reason}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
