# Git Practice — main & develop workflow

Repo: 2 branches → `main` (production), `develop` (integration)

---

## 0. Clone the repo (first time only)

"Local" = a folder on your machine that is a **git repo** (has a hidden `.git` folder tracking history). Cloning creates that folder from the remote (e.g. GitHub).

```bash
git clone https://github.com/<user>/<repo>.git
cd <repo>
git branch -a          # see all branches (local + remote)
```

---

## 1. Feature → develop → main

```bash
# start from develop
git checkout develop
git pull origin develop          # make sure it's up to date

# create feature branch
git checkout -b feature/login-page

# ... make code changes ...

git status                       # see what changed
git add .                        # stage ALL changed files (see note below)
git commit -m "Add login page"
git push origin feature/login-page   # push feature branch to remote

# merge feature into develop
git checkout develop
git pull origin develop
git merge feature/login-page
git push origin develop

# once develop is stable, promote to main
git checkout main
git pull origin main
git merge develop
git push origin main
```

Optional cleanup:
```bash
git branch -d feature/login-page          # delete local feature branch
git push origin --delete feature/login-page   # delete remote feature branch
```

---

## 2. Get latest main → merge into develop

Used when `main` has a hotfix/change that `develop` doesn't have yet.

```bash
git checkout main
git pull origin main             # fetch + merge latest main

git checkout develop
git pull origin develop
git merge main                   # bring main's changes into develop
git push origin develop
```

---

## 3. Key terms

| Term | Meaning |
|---|---|
| `origin` | Nickname for the **remote** repo URL (set automatically when you `clone`). `origin main` = "main branch on the remote called origin". You can have multiple remotes, `origin` is just the default name. |
| `git add .` | Stages **all** modified/new files in the current folder (and subfolders) into the **staging area**, ready to be committed. (`git add file.txt` stages just one file.) |
| `git commit` | Saves staged changes as a snapshot in **local** history. |
| `git push` | Uploads local commits to the remote (`origin`). |
| `git pull` | = `git fetch` + `git merge` → downloads remote commits and merges into your current local branch. |
| `git fetch` | Downloads remote commits/branches but does **not** merge them. |

---

## 4. Quick command reference

```bash
git status                # what's changed / staged
git log --oneline --graph --all   # visual history of all branches
git branch                # list local branches
git checkout <branch>     # switch branch
git checkout -b <branch>  # create + switch
git diff                  # unstaged changes
git diff --staged         # staged changes not yet committed
```

---

## Typical flow (cheat sheet)

```
feature/xyz  --merge-->  develop  --merge-->  main
     ^                       |
     |______ pull latest ____|
```
