# How to Install Skills

This guide explains how to install new skills into your `Segmento-app-website-dev` workspace.

## 1. Find a Skill
Browse [skills.sh](https://skills.sh) or GitHub to find a skill you want to add.
A skill usually consists of a `SKILL.md` file (instructions) and sometimes a `scripts` folder.

## 2. Create the Skill Directory
Navigate to your project folder and create a new directory inside `skills/` with the name of the skill.

**Example (Command Line):**
```powershell
mkdir skills\new-skill-name
```

## 3. Add the Skill Content
Create a `SKILL.md` file inside that new folder and paste the content of the skill.

**File Structure:**
```text
Segmento-app-website-dev/
├── skills/
│   ├── frontend-design/
│   │   └── SKILL.md
│   ├── ui-ux-pro-max/
│   │   └── SKILL.md
│   └── new-skill-name/       <-- New Skill
│       └── SKILL.md          <-- Paste content here
```

## 4. (Optional) Run Scripts
If the skill comes with a setup script (like `setup.sh` or `install.js`), download it to the same folder and run it.

## 5. Usage
Once installed, your AI agent (Antigravity) can be instructed to "read the [skill name] skill" to internalize those capabilities before working on a task.
